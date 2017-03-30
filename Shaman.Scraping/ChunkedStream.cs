using Shaman.Runtime;
using System;
using System.IO;

namespace Shaman.Scraping
{
    public class ChunkedStream : ReadOnlyStreamBase
    {
        private Stream response;

        public ChunkedStream(Stream response)
        {
            this.response = response;
        }

        private long remainingInCurrentChunk;
        private bool started;
        private bool ended;
        public override int Read(byte[] buffer, int offset, int count)
        {
            if (ended) return 0;
            var prevWasZero = false;
            while (true)
            {
                if (remainingInCurrentChunk == 0)
                {
                    if (started)
                    {
                        var cr = response.ReadByte();
                        if (cr != 13) throw new InvalidDataException();
                        var lf = response.ReadByte();
                        if (lf != 10) throw new InvalidDataException();
                    }

                    started = true;
                    long len = 0;
                    var any = false;
                    while (true)
                    {
                        var ch = response.ReadByte();
                        if (ch == (byte)' ') continue;
                        if (ch == -1)
                        {
                            if (prevWasZero && len == 0)
                            {
                                ended = true;
                                return 0;
                            }
                            throw new InvalidDataException();
                        }
                        if (ch == 13)
                        {
                            var n = response.ReadByte();
                            if (n != 10) throw new InvalidDataException();
                            break;
                        }
                        len *= 16;
                        len += ParseHexDigit((char)ch);
                        any = true;
                    }
                    if (!any) return 0;
                    prevWasZero = len == 0;
                    if (len == 0) 
                    {
                        continue;
                    }
                    remainingInCurrentChunk = len;
                }
                var r = response.Read(buffer, offset, (int)Math.Min(count, remainingInCurrentChunk));
                remainingInCurrentChunk -= r;
                return r;
            }
        }

        private int ParseHexDigit(char ch)
        {
            var v = HexUpper.IndexOf(ch);
            if (v != -1) return v;
            v = HexLower.IndexOf(ch);
            if (v != -1) return v;
            throw new InvalidDataException();
        }

        private readonly static string HexUpper = "0123456789ABCDEF";
        private readonly static string HexLower = "0123456789abcdef";

    }
}