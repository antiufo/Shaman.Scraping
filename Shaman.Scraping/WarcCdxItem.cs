using Brotli;
using Shaman.Dom;
using Shaman.Runtime;
using Shaman.Runtime.ReflectionExtensions;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Reflection;
using System.Text;
using System.Text.Utf8;
using System.Threading.Tasks;

namespace Shaman.Scraping
{
    internal class CdxLabelAttribute : Attribute
    {
        public CdxLabelAttribute(string label)
        {
            this.Label = label;
        }
        public string Label { get; }
    }

    

    public class WarcItem
    {
        public string Url;
        public long CompressedOffset;
        public long CompressedLength;
        public long PayloadLength = -1;
        public DateTime Date;
        public string WarcFile;
        public DateTime? LastModified;
        public string ContentType;
        public HttpStatusCode ResponseCode;


        public static IReadOnlyList<WarcItem> ReadIndex(string cdxPath)
        {
            string fileName = null;
            byte[] fileNameBytes = null;
            var list = new List<WarcItem>();
            var folder = Path.GetDirectoryName(cdxPath);
            return WarcCdxItemRaw.Read(cdxPath).Select(x => x.ToWarcItem(folder, ref fileNameBytes, ref fileName)).ToList();
        }


        public long GetPayloadLength()
        {
            lock (this)
            {
                if (PayloadLength != -1) return PayloadLength;

                using (var payload = OpenStream())
                {
                    if (PayloadLength == -1)
                    {
                        var l = 0;
                        var buffer = new byte[16 * 1024];
                        while (true)
                        {
                            var z = payload.Read(buffer, 0, buffer.Length);
                            if (z == 0) break;
                            l += z;
                        }

                        PayloadLength = l;
                    }
                }

                return PayloadLength;
            }
        }

        public string ReadText()
        {
            return ReadText(Encoding.UTF8);
        }

        public HtmlNode ReadHtml()
        {
            return ReadHtml(Encoding.UTF8);
        }




        public string ReadText(Encoding defaultEncoding)
        {
            
            using (var stream = OpenStream((name, value) =>
            {
                if (name == "Content-Type")
                {
                    var charset = value.ToString().TryCaptureAfter("charset=");
                    if (charset == "utf8") charset = "utf-8";
                    if (charset != null)
                    {
                        try
                        {
                            defaultEncoding = Encoding.GetEncoding(charset);
                        }
                        catch
                        {
                        }
                    }
                }
            }))
            {
                using (var sr = new StreamReader(stream, defaultEncoding ?? Encoding.UTF8)) 
                {
                    return sr.ReadToEnd();
                }
            }

        }

        public HtmlNode ReadHtml(Encoding defaultEncoding)
        {
            var doc = new HtmlDocument();
            var headers = new List<KeyValuePair<string, string>>();
            using (var stream = OpenStream((name, value) =>
            {
                headers.Add(new KeyValuePair<string, string>(name.ToString(), value.ToString()));
                if (name == "Content-Type")
                {
                    var charset = value.ToString().TryCaptureAfter("charset=");
                    if (charset == "utf8") charset = "utf-8";
                    if (charset != null)
                    {
                        try
                        {
                            defaultEncoding = Encoding.GetEncoding(charset);
                        }
                        catch
                        {
                        }
                    }
                }
            }))
            {
                doc.Load(stream, defaultEncoding ?? Encoding.UTF8);
            }

#if SHAMAN
            doc.SetPageUrl(this.Url.AsUri());
#else
            doc.PageUrl = new Uri(this.Url);
#endif
            doc.DocumentNode.SetAttributeValue("date-retrieved", this.Date.ToString("o"));

            foreach (var header in headers)
            {
                doc.DocumentNode.SetAttributeValue("header-" + header.Key, header.Value);
            }

            return doc.DocumentNode;
        }

        public Stream OpenStream(Action<Utf8String, Utf8String> onHttpHeader)
        {
            var z = File.Open(WarcFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            z.Position = CompressedOffset;

            var gz = new GZipStream(new LimitedStream(z, CompressedLength), System.IO.Compression.CompressionMode.Decompress);
            var reader = new Utf8StreamReader(gz);
            var warcHeader = reader.ReadTo((Utf8String)"\r\n\r\n");

            var warcContentLength = ReadWarcRecordContentLength(warcHeader);

            var response = OpenHttp(reader, null, new Uri(this.Url), warcContentLength, out var payloadLength, out var redirectLocation, out var responseCode, out var contentType, out var lastModified, onHttpHeader);
            if (payloadLength != -1)
                PayloadLength = payloadLength;
            return response;
        }
        
        public Stream OpenStream()
        {
            return OpenStream(null);
        }

        private static long ReadWarcRecordContentLength(Utf8String warcHeader)
        {
            return Utf8Utils.ParseInt64(warcHeader.CaptureBetween((Utf8String)"Content-Length:", (Utf8String)"\n").Trim());
        }

     


        private readonly static Utf8String Http_TransferEncoding = new Utf8String("Transfer-Encoding:");
        private readonly static Utf8String Http_LastModified = new Utf8String("Last-Modified:");
        private readonly static Utf8String Http_ContentEncoding = new Utf8String("Content-Encoding:");
        private readonly static Utf8String Http_Location = new Utf8String("Location:");
        private readonly static Utf8String Http_ContentType = new Utf8String("Content-Type:");
        private readonly static Utf8String Http_ContentLength = new Utf8String("Content-Length:");
        private readonly static Utf8String Http_Gzip = new Utf8String("gzip");
        private readonly static Utf8String Http_Brotli = new Utf8String("br");

        public static Stream OpenHttp(Utf8StreamReader httpReader, Scratchpad scratchpad, Uri requestedUrl, long responseLength, out long payloadLength, out Uri location, out int responseCode, out Utf8String contentType, out DateTime? lastModified, Action<Utf8String, Utf8String> onHttpHeader)
        {
            var startPosition = httpReader.Position;
            payloadLength = -1;
            location = null;
            lastModified = null;

            bool chunked = false;
            bool gzipped = false;
            bool brotli = false;
            var responseLine = httpReader.ReadLine();

            responseCode = (int)Utf8Utils.ParseInt64(responseLine.TryCaptureBetween((byte)' ', (byte)' ') ?? responseLine.CaptureAfter((byte)' '));
            while (true)
            {
                var line = httpReader.ReadLine();
                if (httpReader.IsCompleted) throw new InvalidDataException();
                if (line.Length == 0) break;
                if (onHttpHeader != null)
                {
                    var d = line.IndexOf((byte)':');
                    onHttpHeader(line.Substring(0, d).Trim(), line.Substring(d + 1).Trim());
                }
                if (line.StartsWith(Http_TransferEncoding))
                {
                    var value = GetHeaderValue(line);
                    if (value.Equals("chunked")) chunked = true;
                }
                else if (line.StartsWith(Http_ContentLength))
                {
                    payloadLength = Utf8Utils.ParseInt64(GetHeaderValue(line));
                }
                else if (line.StartsWith(Http_ContentEncoding))
                {
                    var value = GetHeaderValue(line);
                    if (value == Http_Gzip) gzipped = true;
                    else if (value == Http_Brotli) brotli = true;
                }
                else if (line.StartsWith(Http_Location))
                {
                    var val = GetHeaderValue(line).ToString();
                    if (val.StartsWith("//")) location = new Uri(requestedUrl.Scheme + ":" + val);
                    else location = new Uri(requestedUrl, val);
                }
                else if (line.StartsWith(Http_ContentType) && scratchpad != null)
                {
                    var value = GetHeaderValue(line);
                    value = value.TryCaptureBefore((byte)' ') ?? value;
                    value = value.TryCaptureBefore((byte)';') ?? value;
                    contentType = scratchpad.Copy(value);
                }
                else if (line.StartsWith(Http_LastModified))
                {
                    try
                    {
                        lastModified = WarcCdxItemRaw.ParseHttpDate(GetHeaderValue(line));
                    }
                    catch { }
                }
            }

            var compressed = gzipped || brotli;
            var currentPos = httpReader.Position - startPosition;
            var httpBodyLength = responseLength - currentPos;
            if (compressed || chunked) payloadLength = -1;
            if (!compressed && !chunked && payloadLength != -1 && httpBodyLength != payloadLength)
            {
                throw new Exception("Unexpected Content-Length.");
            }
            Stream s = new LimitedStream(httpReader, httpBodyLength);
            if (chunked) s = new ChunkedStream(s);
            if (compressed && chunked) s = new OnDisposeConsumeStream(s);

            if (gzipped) s = new GZipStream(s, CompressionMode.Decompress);
            else if (brotli) s = new BrotliStream(s, CompressionMode.Decompress);

            return s;
        }

        internal static Utf8String GetHeaderValue(Utf8String line)
        {
            return line.CaptureAfter((byte)':').Trim();
        }

        private class OnDisposeConsumeStream : ReadOnlyStreamBase
        {
            private Stream s;
            private bool ended;
            public OnDisposeConsumeStream(Stream s)
            {
                this.s = s;
            }
            public override int Read(byte[] buffer, int offset, int count)
            {
                var k = s.Read(buffer, offset, count);
                ended = k == 0;
                return k;
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    if (!ended)
                    {
                        var buffer = new byte[16 * 1024];
                        while (true)
                        {
                            var r = s.Read(buffer, 0, buffer.Length);
                            if (r == 0)
                            {
                                ended = true;
                                break;
                            }
                            else
                            {

                            }
                        }
                        s.Dispose();
                    }
                }
            }
        }
    }
    internal class WarcCdxItemRaw
    {

        
        public WarcItem ToWarcItem(string folder, ref byte[] fileNameBytes, ref string fileNameString)
        {
            string fn;
            if (fileNameBytes != null && fileNameBytes.Length == this.FileName.Length && this.FileName.Bytes.BlockEquals((ReadOnlySpan<byte>)fileNameBytes.Slice()))
            {
                fn = fileNameString;
            }
            else
            {
                fileNameBytes = new byte[this.FileName.Length];
                this.FileName.Bytes.CopyTo(fileNameBytes);
                fileNameString = Path.Combine(folder, this.FileName.ToString());
                fn = fileNameString;
            }

            return new WarcItem()
            {
                Url = this.OriginalUrl.ToString(),
                CompressedOffset = Utf8Utils.ParseInt64(this.CompressedArcFileOffset),
                CompressedLength = Utf8Utils.ParseInt64(this.CompressedRecordSize),
                Date = ParseDate(this.Date),
                PayloadLength = this.PayloadLength.Length != 0 ? Utf8Utils.ParseInt64(this.PayloadLength) : -1,
                WarcFile = fn,
                LastModified = this.LastModified.Length > 1 ? ParseDate(this.LastModified) : (DateTime?)null,
                ContentType = this.MimeTypeOfOriginalDocument.ToStringCached(),
                ResponseCode = this.ResponseCode.Length != 0 ? (HttpStatusCode)Utf8Utils.ParseInt32( this.ResponseCode) : default(HttpStatusCode),
            };

        }

        private DateTime ParseDate(Utf8String date)
        {
            if (date.Length == 14)
            {
                return Utf8Utils.ParseDateConcatenated(date);
            }
            var num = Utf8Utils.ParseInt64(date);
            if (num < 1980_00_00_000000) return new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(num);
            throw new ArgumentException();
        }

        public static IEnumerable<WarcCdxItemRaw> Read(string v)
        {
            return Read(File.Open(v, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete));
        }

        private static Dictionary<Utf8String, Action<WarcCdxItemRaw, Utf8String>> allSetters;

        private static readonly Utf8String CDX = new Utf8String("CDX");
        public static IEnumerable<WarcCdxItemRaw> Read(Stream cdxStream)
        {
            using (var reader = new Utf8StreamReader(cdxStream))
            {
                var fields = reader.ReadLine().Split((byte)' ');

                if (allSetters == null)
                {
                    var dict = new Dictionary<Utf8String, Action<WarcCdxItemRaw, Utf8String>>();
                    foreach (var field in typeof(WarcCdxItemRaw).GetFields(BindingFlags.Public | BindingFlags.Instance))
                    {
                        var attr = field.GetCustomAttribute<CdxLabelAttribute>();
                        if (attr != null)
                        {
                            var this_ = Expression.Parameter(typeof(WarcCdxItemRaw), "this_");
                            var value = Expression.Parameter(typeof(Utf8String), "value");
                            dict[new Utf8String(attr.Label)] = Expression.Lambda<Action<WarcCdxItemRaw, Utf8String>>(Expression.Assign(Expression.Field(this_, field), value), this_, value).Compile();
                        }
                    }
                    allSetters = dict;
                }

                var fieldSetters = new List<Action<WarcCdxItemRaw, Utf8String>>();

                var foundCdx = false;
                foreach (var label in fields)
                {
                    if (!foundCdx)
                    {
                        if (label == CDX)
                            foundCdx = true;
                    }
                    else
                    {
                        allSetters.TryGetValue(label, out var setter);
                        fieldSetters.Add(setter);
                    }
                }


                Utf8String[] arr = null;
                while (!reader.IsCompleted)
                {
                    var line = reader.ReadLine();
                    if (line.Length == 0) continue;

                    line.Split((byte)' ', StringSplitOptions.None, ref arr);


                    var item = new WarcCdxItemRaw();
                    for (int i = 0; i < fieldSetters.Count; i++)
                    {
                        fieldSetters[i]?.Invoke(item, arr[i]);
                    }
                    yield return item;
                    //action(item);
                }

            }
        }

        public static void GenerateCdx(string dir)
        {
            GenerateCdx(Path.Combine(dir, "index.cdx"), Directory.EnumerateFiles(dir, "*.warc.gz"));
        }

        internal readonly static string WarcColumns = " CDX a V S b g s m PayloadLength LastModified";
        public static void GenerateCdx(string cdx, IEnumerable<string> warcs)
        {
            var scratchpad = new Scratchpad();
            var buf = new byte[16 * 1024];
            using (var output = File.Open(cdx + ".tmp", FileMode.Create, FileAccess.Write, FileShare.Delete | FileShare.Read))
            {
                using (var writer = new Utf8StreamWriter(output))
                {
                    writer.WriteClrStringLine(WarcColumns);
                    foreach (var warc in warcs)
                    {
                        Console.WriteLine(Path.GetFileName(warc));

                        using (var warcStream = File.Open(warc, FileMode.Open, FileAccess.Read, FileShare.Delete | FileShare.Read))
                        {
                            try
                            {
                                var warcname = new Utf8String(Path.GetFileName(warc));
                                while (warcStream.Position != warcStream.Length)
                                {
                                    var startPosition = warcStream.Position;
                                    long warcContentLength = -1;
                                    long payloadLength = -1;
                                    int responseCode = -1;
                                    var contentType = Utf8String.Empty;
                                    var date = scratchpad.Use(14);
                                    date[0] = 0;
                                    Utf8String url = Utf8String.Empty;
                                    Utf8String shamanUrl = Utf8String.Empty;
                                    DateTime? lastModified = null;
                                    bool isresponse = false;
                                    using (var gz = new GZipStream(warcStream, CompressionMode.Decompress, true))
                                    {
                                        using (var reader = new Utf8StreamReader(gz, true))
                                        {
                                            while (true)
                                            {
                                                if (reader.IsCompleted) throw new EndOfStreamException();
                                                var line = reader.ReadLine();
                                                if (line.Length == 0) break;
                                                if (line.Equals(Warc_Response)) isresponse = true;
                                                if (line.StartsWith(Warc_ContentLength))
                                                {
                                                    warcContentLength = Utf8Utils.ParseInt64(WarcItem.GetHeaderValue(line));
                                                }
                                                else if (line.StartsWith(Warc_Date))
                                                {
                                                    var val = WarcItem.GetHeaderValue(line).Bytes;
                                                    val.Slice(0, 4).CopyTo(date.Slice(0));
                                                    val.Slice(5, 2).CopyTo(date.Slice(4));
                                                    val.Slice(8, 2).CopyTo(date.Slice(6));
                                                    val.Slice(11, 2).CopyTo(date.Slice(8));
                                                    val.Slice(14, 2).CopyTo(date.Slice(10));
                                                    val.Slice(17, 2).CopyTo(date.Slice(12));
                                                }
                                                else if (line.StartsWith(Warc_URL))
                                                {
                                                    url = scratchpad.Copy(WarcItem.GetHeaderValue(line));
                                                }
                                                if (line.StartsWith(Warc_Shaman_URI))
                                                {
                                                    shamanUrl = scratchpad.Copy(WarcItem.GetHeaderValue(line));
                                                }
                                            }
                                            if (warcContentLength == -1) throw new InvalidOperationException();


                                            if (isresponse)
                                            {
                                                using (var s = WarcItem.OpenHttp(reader, scratchpad, new Uri(url.ToString()), warcContentLength, out var payloadLengthFromHeader, out var redirectLocation, out responseCode, out contentType, out lastModified, null))
                                                {
                                                    long l = 0;
                                                    while (true)
                                                    {
                                                        var m = s.Read(buf, 0, buf.Length);
                                                        if (m == 0) break;
                                                        l += m;
                                                    }
                                                    payloadLength = l;
                                                    if (payloadLengthFromHeader != -1 && payloadLengthFromHeader != payloadLength) throw new Exception("Content-Length mismatch.");
                                                }
                                                //var httpData = new LimitedStream(reader, contentLength);
                                                var cr = reader.ReadByte();
                                                if (cr != 13) throw new InvalidDataException();
                                                var lf = reader.ReadByte();
                                                if (lf != 10) throw new InvalidDataException();


                                                cr = reader.ReadByte();
                                                if (cr != 13) throw new InvalidDataException();
                                                lf = reader.ReadByte();
                                                if (lf != 10) throw new InvalidDataException();
                                                //if (reader.ReadByte() != 13) throw new Exception();
                                                //if (reader.ReadByte() != 10) throw new Exception();

                                            }
                                            else
                                            {
                                                var remaining = warcContentLength;
                                                while (remaining != 0)
                                                {
                                                    var m = reader.Read((int)Math.Min(remaining, int.MaxValue));
                                                    if (m.Count == 0) throw new Exception();
                                                    remaining -= m.Count;
                                                }

                                                var e = reader.ReadLine();
                                                if (e.Length != 0) throw new InvalidDataException();
                                                e = reader.ReadLine();
                                                if (e.Length != 0) throw new InvalidDataException();
                                                e = reader.ReadLine();
                                                if (!reader.IsCompleted) throw new InvalidDataException();

                                            }

                                            //var r = reader.RemainingBufferedData;
                                            var end = reader.ReadByte();
                                            if (end != -1) throw new InvalidDataException();
                                            //Console.WriteLine($"Remaining: {r.Length}");

                                        }




                                        var deflateStream = gz.GetFieldOrProperty("deflateStream");
                                        var buffer = deflateStream.GetFieldOrProperty("buffer");
                                        var inflater = deflateStream.GetFieldOrProperty("inflater");
                                        var input = inflater.GetFieldOrProperty("input");
                                        var b = (int)input.GetFieldOrProperty("AvailableBytes");
                                        warcStream.Position -= b;
                                    }



                                    if (isresponse)
                                    {
                                        if (shamanUrl.Length > 0) writer.Write(shamanUrl);
                                        else writer.Write(url);
                                        writer.Write((byte)' ');
                                        writer.Write(startPosition);
                                        writer.Write((byte)' ');
                                        writer.Write(warcStream.Position - startPosition);
                                        writer.Write((byte)' ');
                                        if (date[0] != 0) writer.Write(date);
                                        else writer.Write((byte)'-');
                                        writer.Write((byte)' ');
                                        writer.Write(warcname);
                                        writer.Write((byte)' ');
                                        if (responseCode != -1) writer.Write(responseCode);
                                        else writer.Write((byte)'-');
                                        writer.Write((byte)' ');
                                        writer.Write(contentType);
                                        writer.Write((byte)' ');
                                        writer.Write(payloadLength);
                                        writer.Write((byte)' ');
                                        if (lastModified != null)
                                        {
                                            WriteDate(writer, lastModified.Value);
                                        }
                                        else writer.Write((byte)'-');
                                        writer.Write((byte)' ');

                                        writer.WriteLine();
                                    }
                                    scratchpad.Reset();
                                }
                            }
                            catch
                            {
                                if (warcStream.Position == warcStream.Length)
                                {
                                    Console.WriteLine("WARNING: truncated WARC."); ;
                                }
                                else
                                {
                                    throw;
                                }
                            }
                        }
                    }
                }
            }
            File.Delete(cdx);
            File.Move(cdx + ".tmp", cdx);
        }

        private static void WriteDate(Utf8StreamWriter writer, DateTime date)
        {
            writer.Write(date.Year);
            WriteTwoDigitValue(writer, date.Month);
            WriteTwoDigitValue(writer, date.Day);
            WriteTwoDigitValue(writer, date.Hour);
            WriteTwoDigitValue(writer, date.Minute);
            WriteTwoDigitValue(writer, date.Second);
        }

        private static void WriteTwoDigitValue(Utf8StreamWriter writer, int num)
        {
            if (num < 10) writer.Write((byte)'0');
            writer.Write(num);
        }

        internal static DateTime ParseHttpDate(Utf8String str)
        {
            Utf8Utils.ReadTo(ref str, (byte)' ');
            var day = Utf8Utils.ParseInt32(Utf8Utils.ReadTo(ref str, (byte)' '));
            var month = ParseMonth(Utf8Utils.ReadTo(ref str, (byte)' '));
            var year = Utf8Utils.ParseInt32(Utf8Utils.ReadTo(ref str, (byte)' '));

            //str.Split((byte)' ', StringSplitOptions.None, ref arr);
            return new DateTime(
                year,
                month,
                day,
                Utf8Utils.ParseInt32(Utf8Utils.ReadTo(ref str, (byte)':')),
                Utf8Utils.ParseInt32(Utf8Utils.ReadTo(ref str, (byte)':')),
                Utf8Utils.ParseInt32(Utf8Utils.ReadTo(ref str, (byte)' ')),
                DateTimeKind.Utc
                );
        }

        private static int ParseMonth(Utf8String month)
        {
            var firstChar = month[0];
            var third = month[2];
            switch (firstChar)
            {
                case (byte)'J' when month[1] == (byte)'a': return 1;
                case (byte)'F': return 2;
                case (byte)'M' when third == (byte)'r': return 3;
                case (byte)'A' when third == (byte)'r': return 4;
                case (byte)'M' when third == (byte)'y': return 5;
                case (byte)'J' when third == (byte)'n': return 6;
                case (byte)'J' when third == (byte)'l': return 7;
                case (byte)'A' when third == (byte)'g': return 8;
                case (byte)'S': return 9;
                case (byte)'O': return 10;
                case (byte)'N': return 11;
                case (byte)'D': return 12;
                default: throw new FormatException();
            }
            
        }

      

        private readonly static Utf8String Warc_ContentLength = new Utf8String("Content-Length:");
        private readonly static Utf8String Warc_Date = new Utf8String("WARC-Date:");
        private readonly static Utf8String Warc_Response = new Utf8String("WARC-Type: response");
        private readonly static Utf8String Warc_URL = new Utf8String("WARC-Target-URI:");
        private readonly static Utf8String Warc_Shaman_URI = new Utf8String("WARC-Shaman-URI:");


        [CdxLabel("A")] public Utf8String CanonizedUrl;
        [CdxLabel("B")] public Utf8String NewsGroup;
        [CdxLabel("C")] public Utf8String RulespaceCategory;
        [CdxLabel("D")] public Utf8String CompressedDatFileOffset;
        [CdxLabel("F")] public Utf8String CanonizedFrame;
        [CdxLabel("G")] public Utf8String MultiColummLanguageDescription;
        [CdxLabel("H")] public Utf8String CanonizedHost;
        [CdxLabel("I")] public Utf8String CanonizedImage;
        [CdxLabel("J")] public Utf8String CanonizedJumpPoint;
        [CdxLabel("K")] public Utf8String SomeWeirdFBISWhatSChangedKindaThing;
        [CdxLabel("L")] public Utf8String CanonizedLink;
        [CdxLabel("M")] public Utf8String MetaTagsAIF;
        [CdxLabel("N")] public Utf8String MassagedUrl;
        [CdxLabel("P")] public Utf8String CanonizedPath;
        [CdxLabel("Q")] public Utf8String LanguageString;
        [CdxLabel("R")] public Utf8String CanonizedRedirect;
        [CdxLabel("U")] public Utf8String Uniqness;
        [CdxLabel("V")] public Utf8String CompressedArcFileOffset;
        [CdxLabel("X")] public Utf8String CanonizedUrlInOtherHrefTages;
        [CdxLabel("Y")] public Utf8String CanonizedUrlInOtherSrcTags;
        [CdxLabel("Z")] public Utf8String CanonizedUrlFoundInScript;
        [CdxLabel("a")] public Utf8String OriginalUrl;
        [CdxLabel("b")] public Utf8String Date;
        [CdxLabel("c")] public Utf8String OldStyleChecksum;
        [CdxLabel("d")] public Utf8String UncompressedDatFileOffset;
        [CdxLabel("e")] public Utf8String IP;
        [CdxLabel("f")] public Utf8String Frame;
        [CdxLabel("g")] public Utf8String FileName;
        [CdxLabel("h")] public Utf8String OriginalHost;
        [CdxLabel("i")] public Utf8String Image;
        [CdxLabel("j")] public Utf8String OriginalJumpPoint;
        [CdxLabel("k")] public Utf8String NewStyleChecksum;
        [CdxLabel("l")] public Utf8String Link;
        [CdxLabel("m")] public Utf8String MimeTypeOfOriginalDocument;
        [CdxLabel("n")] public Utf8String ArcDocumentLength;
        [CdxLabel("o")] public Utf8String Port;
        [CdxLabel("p")] public Utf8String OriginalPath;
        [CdxLabel("r")] public Utf8String Redirect;
        [CdxLabel("s")] public Utf8String ResponseCode;
        [CdxLabel("t")] public Utf8String Title;
        [CdxLabel("v")] public Utf8String UncompressedArcFileOffset;
        [CdxLabel("x")] public Utf8String UrlInOtherHrefTages;
        [CdxLabel("y")] public Utf8String UrlInOtherSrcTags;
        [CdxLabel("z")] public Utf8String UrlFoundInScript;

        [CdxLabel("S")] public Utf8String CompressedRecordSize;
        [CdxLabel("u")] public Utf8String Urn;

        [CdxLabel("PayloadLength")] public Utf8String PayloadLength;
        [CdxLabel("LastModified")] public Utf8String LastModified;

        internal static void AppendCdx(string directory, List<WarcItem> warcItemsToAppend)
        {
            var cdx = Path.Combine(directory, "index.cdx");
            if (File.Exists(cdx))
            {
                using (var reader = new Utf8StreamReader(cdx))
                {
                    var line = reader.ReadLine();
                    if (line != WarcColumns) throw new Exception("The columns of the CDX are different, cannot append.");
                }
            }

            using (var stream = File.Open(cdx, FileMode.Append, FileAccess.Write, FileShare.Delete))
            using (var writer = new Utf8StreamWriter(stream))
            {
                if (stream.Length == 0) writer.WriteClrStringLine(WarcColumns);
                foreach (var warcItem in warcItemsToAppend)
                {

                    writer.WriteClrString(warcItem.Url);
                    writer.Write((byte)' ');
                    writer.Write(warcItem.CompressedOffset);
                    writer.Write((byte)' ');
                    writer.Write(warcItem.CompressedLength);
                    writer.Write((byte)' ');
                    if (warcItem.Date != default(DateTime) ) WriteDate(writer, warcItem.Date);
                    else writer.Write((byte)'-');
                    writer.Write((byte)' ');

                    var warcName = warcItem.WarcFile.IndexOf('\\') != -1 || warcItem.WarcFile.IndexOf('/') != -1 ? Path.GetFileName(warcItem.WarcFile) : warcItem.WarcFile;
                    writer.WriteClrString(warcName);
                    writer.Write((byte)' ');
                    if (warcItem.ResponseCode > 0) writer.Write((int)warcItem.ResponseCode);
                    else writer.Write((byte)'-');
                    writer.Write((byte)' ');
                    writer.WriteClrString(warcItem.ContentType);
                    writer.Write((byte)' ');
                    writer.Write(warcItem.PayloadLength);
                    writer.Write((byte)' ');
                    if (warcItem.LastModified != null)
                    {
                        WriteDate(writer, warcItem.LastModified.Value);
                    }
                    else writer.Write((byte)'-');
                    writer.Write((byte)' ');

                    writer.WriteLine();


                }
            }


        }
    }


}
