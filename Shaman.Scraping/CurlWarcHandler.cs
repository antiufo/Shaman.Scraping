using CurlSharp;
using Shaman.Dom;
using Shaman.Scraping;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Shaman.Runtime
{
    class CurlWarcHandler : HttpClientHandler
    {
        public override bool SupportsAutomaticDecompression => true;

        public Action<HtmlNode, Uri, Exception> OnHtmlRetrieved;
        public Action<HttpResponseMessage, CurlEasy, MemoryStream, MemoryStream> OnResponseReceived;

        private object lockObj = new object();
        private bool disposed;

        public CurlWarcHandler()
        {
            Curl.GlobalInit(CurlInitFlag.All);
        }

        protected override void Dispose(bool disposing)
        {
            if (!disposing) return;
            lock (lockObj)
            {
                if (disposed) return;
                foreach (var item in pooledEasyHandles)
                {
                    item.Dispose();
                }

                foreach (var item in pooledRequestMemoryStreams)
                {
                    item.Dispose();
                }
                foreach (var item in pooledResponseMemoryStreams)
                {
                    item.Dispose();
                }
                disposed = true;
                pooledEasyHandles = null;
                pooledRequestMemoryStreams = null;
                pooledResponseMemoryStreams = null;
            }

        }

        private List<CurlEasy> pooledEasyHandles = new List<CurlEasy>();
        private List<MemoryStream> pooledRequestMemoryStreams = new List<MemoryStream>();
        private List<MemoryStream> pooledResponseMemoryStreams = new List<MemoryStream>();

        // Must hold lock
        private T BorrowPooled<T>(List<T> pool) where T : new()
        {
            if (pool.Count == 0) return new T();
            var m = pool.Last();
            pool.RemoveAt(pool.Count - 1);
            return m;
        }

        internal object syncObj;
        internal Func<Uri, CurlEasy, MemoryStream, MemoryStream, WarcWriter> GetDestinationWarc;
        internal Func<HttpRequestMessage, HttpResponseMessage> TryGetCached;
        // internal Func<SynchronizationContext> GetSynchronizationContext;

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            // If everything is happening cooperatively, no problem. No need to specify a syncctx.
            // If we arrive here from a sync XHR, the main thread will be blocked waiting for the task on the background thread (that is, this one).
            // in that case, there is no need to lock or dispatch on another syncctx, because that syncctx is blocked/busy.
            // So, don't use a syncctx in either cases.


            /*
            var syncCtx = GetSynchronizationContext?.Invoke();
            if (syncCtx != null)
            {
                Task<HttpResponseMessage> m = null;
                syncCtx.Send(() => { m = SendAsyncInternal(request, cancellationToken); });
                return m;
            }
            else
            */
            {
                return SendAsyncInternal(request, cancellationToken);
            }
        }

        private async Task<HttpResponseMessage> SendAsyncInternal(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (disposed) throw new ObjectDisposedException(nameof(CurlWarcHandler));
            if (request.Properties.TryGetValue("ShamanURL", out var shamanUrlObj) && shamanUrlObj is LazyUri shamanUrl)
            {
                shamanUrl.RemoveFragmentParameter("$assume-text");
                request.Properties["ShamanURL"] = shamanUrl;
            }
            if (TryGetCached != null)
            {
                var cached = TryGetCached(request);
                if (cached != null) return cached;
                else { }
            }

            CurlEasy easy = null;
            MemoryStream requestMs = null;
            MemoryStream responseMs = null;
            lock (lockObj)
            {
                easy = BorrowPooled(pooledEasyHandles);
                requestMs = BorrowPooled(pooledRequestMemoryStreams);
                responseMs = BorrowPooled(pooledResponseMemoryStreams);
            }
            Sanity.Assert(requestMs != null);

            var response = new HttpResponseMessage();



            var (httpCode, curlCode, warcItem) = await WebsiteScraper.ScrapeAsync(easy, request, request.RequestUri.AbsoluteUri, requestMs, responseMs, ea =>
            {
                return GetDestinationWarc(request.RequestUri, easy, requestMs, responseMs);
            }, syncObj, cancellationToken);
            if (curlCode != CurlCode.Ok)
            {
                Release(easy, requestMs, responseMs);
                throw new WebException("Curl: " + curlCode, (WebExceptionStatus)(800 + curlCode));
            }

            responseMs.Seek(0, SeekOrigin.Begin);
            var httpResponse = new Utf8StreamReader(responseMs);

            response.RequestMessage = request;
            response.StatusCode = httpCode;

            using (var scratchpad = new Scratchpad())
            {

                var stream = WarcItem.OpenHttp(httpResponse, scratchpad, request.RequestUri, responseMs.Length, out long payloadLength, out var _, out var _, out var contentType, out var _, (key, val) =>
                {
                    response.Headers.TryAddWithoutValidation(key.ToString(), val.ToString());
                });
                response.Content = new System.Net.Http.StreamContent(new DisposeCallbackStream(stream, () =>
                {
                    Release(easy, requestMs, responseMs);
                }));
            }
            OnResponseReceived?.Invoke(response, easy, requestMs, responseMs);
            return response;
        }

        private class DisposeCallbackStream : ReadOnlyStreamBase
        {
            public DisposeCallbackStream(Stream stream, Action disposeCallback)
            {
                this.stream = stream;
                this.disposeCallback = disposeCallback;
            }
            private Stream stream;
            private Action disposeCallback;
            public override int Read(byte[] buffer, int offset, int count)
            {
                return stream.Read(buffer, offset, count);
            }

            protected override void Dispose(bool disposing)
            {
                var s = Interlocked.Exchange<Stream>(ref stream, null);

                if (s != null)
                {
                    disposeCallback();
                }
            }


        }

        private void Release(CurlEasy easy, MemoryStream requestMs, MemoryStream responseMs)
        {
            lock (lockObj)
            {
                if (disposed)
                {
                    easy?.Dispose();
                    requestMs?.Dispose();
                    responseMs?.Dispose();
                    return;
                }
                if (pooledEasyHandles != null && easy != null)
                    pooledEasyHandles.Add(easy);
                if (pooledRequestMemoryStreams != null && requestMs != null)
                    pooledRequestMemoryStreams.Add(requestMs);
                if (pooledResponseMemoryStreams != null && responseMs != null)
                    pooledResponseMemoryStreams.Add(responseMs);
            }
        }
    }
}
