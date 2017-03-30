using Shaman.Runtime;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Utf8;
using System.Threading.Tasks;

namespace Shaman.Scraping
{
    internal class WarcWriter : IDisposable
    {
        public WarcWriter(Stream outstream)
        {
            this.outstream = outstream;
            this.ms = new MemoryStream();
        }

        internal int num;
        public long Length => outstream.Length;

        private Utf8StreamWriter currentRecord;
        private MemoryStream ms;


        private bool UseGzip = true;
        public void StartRecord()
        {
            if (currentRecord != null) throw new InvalidOperationException();
            if (UseGzip)
            {
                var gz = new GZipStream(outstream, CompressionMode.Compress, true);
                currentRecord = new Utf8StreamWriter(gz, false);
            }
            else
            {
                currentRecord = new Utf8StreamWriter(outstream, true);
            }

        }

        internal List<WarcItem> recordedResponses = new List<WarcItem>();
        private byte[] lengthCalculationBuffer = new byte[4096];

        public WarcItem WriteRecord(string url, bool isresponse, MemoryStream req, DateTime date, string ip, string recordId, string concurrentTo, LazyUri shamanUrl)
        {
            var initialPosition = outstream.Position;
            StartRecord();
            currentRecord.WriteClrStringLine("WARC/1.0");
            if (isresponse) currentRecord.WriteClrStringLine("WARC-Type: response");
            else currentRecord.WriteClrStringLine("WARC-Type: request");
            if (isresponse) currentRecord.WriteClrStringLine("Content-Type: application/http;msgtype=response");
            else currentRecord.WriteClrStringLine("Content-Type: application/http;msgtype=request");
            currentRecord.WriteClrString("WARC-Date: ");
            currentRecord.WriteClrString(date.ToString("o").Substring(0, 19));
            currentRecord.WriteClrStringLine("Z");
            currentRecord.WriteClrString("WARC-Record-ID: <urn:uuid:");
            currentRecord.WriteClrString(recordId);
            currentRecord.WriteClrStringLine(">");
            currentRecord.WriteClrString("WARC-Target-URI: ");
            currentRecord.WriteClrStringLine(url);
            if (shamanUrl != null)
            {
                var abs = shamanUrl.AbsoluteUri;
                if (abs != url)
                {
                    currentRecord.WriteClrString("WARC-Shaman-URI: ");
                    currentRecord.WriteClrStringLine(abs);
                }
            }
            currentRecord.WriteClrString("WARC-IP-Address: ");
            currentRecord.WriteClrStringLine(ip);
            if (concurrentTo != null)
            {
                currentRecord.WriteClrString("WARC-Concurrent-To: <urn:uuid:");
                currentRecord.WriteClrString(concurrentTo);
                currentRecord.WriteClrStringLine(">");
            }
            currentRecord.WriteClrString("Content-Length: ");
            currentRecord.Write(req.Length);
            currentRecord.WriteLine();
            currentRecord.WriteClrString("WARC-Warcinfo-ID: <urn:uuid:");
            currentRecord.WriteClrString(WarcInfoId);
            currentRecord.WriteClrStringLine(">");
            currentRecord.WriteLine();
            req.TryGetBuffer(out var buf);
            currentRecord.Write(buf.Array.Slice(buf.Offset, (int)req.Length));
            EndRecord();
            if (isresponse)
            {
                req.Seek(0, SeekOrigin.Begin);
                scratchpad.Reset();
                using (var http = new Utf8StreamReader(req, true))
                {
                    using (var s = WarcItem.OpenHttp(http, scratchpad, url.AsUri(), req.Length, out var payloadLength, out var location, out var responseCode, out var contentType, out var lastModified, null))
                    {
                        if (payloadLength == -1)
                        {
                            var l = 0;
                            while (true)
                            {
                                var m = s.Read(lengthCalculationBuffer, 0, lengthCalculationBuffer.Length);
                                if (m == 0) break;
                                l += m;
                            }
                            payloadLength = l;
                        }

                        var warcItem = new WarcItem()
                        {
                            Url = shamanUrl?.AbsoluteUri ?? url,
                            Date = date,
                            ContentType = contentType.ToStringCached(),
                            LastModified = lastModified,
                            PayloadLength = payloadLength,
                            ResponseCode = (HttpStatusCode)responseCode,
                            CompressedLength = outstream.Position - initialPosition,
                            CompressedOffset = initialPosition,
                            WarcFile = WarcName
                        };
                        recordedResponses.Add(warcItem);
                        onNewWarcItem?.Invoke(warcItem);
                        return warcItem;
                    }
                }

            }

            return null;
        }
        public Action<WarcItem> onNewWarcItem;
        private Scratchpad scratchpad = new Scratchpad();
        private string WarcInfoId;
        internal string WarcName;

        public void EndRecord()
        {
            currentRecord.WriteLine();
            currentRecord.WriteLine();
            currentRecord.Dispose();
            currentRecord = null;
        }


        public void WriteWarcInfo()
        {
            StartRecord();




            ms.SetLength(0);
            using (var warcinfo = new Utf8StreamWriter(ms, true))
            {
                warcinfo.WriteClrStringLine("Software: Shaman.IO/1.1");
                warcinfo.WriteClrStringLine("Format: WARC File Format 1.0");
                warcinfo.WriteClrStringLine("Conformsto: http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf");
                warcinfo.WriteClrStringLine("Robots: off");
                warcinfo.WriteLine();
            }

            currentRecord.WriteClrStringLine("WARC/1.0");
            currentRecord.WriteClrStringLine("WARC-Type: warcinfo");
            currentRecord.WriteClrStringLine("Content-Type: application/warc-fields");

            currentRecord.WriteClrString("WARC-Date: ");
            currentRecord.WriteClrString(DateTime.UtcNow.ToString("o").Substring(0, 19));
            currentRecord.WriteClrStringLine("Z");

            WarcInfoId = Guid.NewGuid().ToString();


            currentRecord.WriteClrString("WARC-Record-ID: <urn:uuid:");
            currentRecord.WriteClrString(WarcInfoId);
            currentRecord.WriteClrStringLine(">");

            currentRecord.WriteClrString("Content-Length: ");
            currentRecord.Write(ms.Length);
            currentRecord.WriteLine();


            currentRecord.WriteClrString("WARC-Warcinfo-ID: <urn:uuid:");
            currentRecord.WriteClrString(WarcInfoId);
            currentRecord.WriteClrStringLine(">");

            currentRecord.WriteLine();

            ms.TryGetBuffer(out var buf);
            currentRecord.Write(buf.Array.Slice(buf.Offset, (int)ms.Length));
            EndRecord();
        }

        public void Flush()
        {
            outstream.Flush();
        }

        private Stream outstream;
        internal string FullName;

        public void Dispose()
        {
            outstream.Dispose();
        }
    }
}
