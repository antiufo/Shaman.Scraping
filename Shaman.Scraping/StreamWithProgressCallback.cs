using Shaman.Runtime;
using Shaman.Types;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shaman.Runtime
{
    public class StreamWithProgressCallback : Stream
    {
        private Stream stream;
        private Action<long> callback;
        private Action onDispose;
        private long position;
        public StreamWithProgressCallback(Stream stream, IProgress<SimpleProgress> progress)
            : this(stream, progress, (long?)null)
        {
        }
        public StreamWithProgressCallback(Stream stream, IProgress<DataTransferProgress> progress)
            : this(stream, progress, (long?)null)
        {
        }
        public StreamWithProgressCallback(Stream stream, IProgress<SimpleProgress> progress, long? length)
        {
            InitStream(stream);
            if (progress != null)
            {
                InitLength(length);
                InitCallback(progress);
            }
        }
        public StreamWithProgressCallback(Stream stream, IProgress<DataTransferProgress> progress, long? length)
        {
            InitStream(stream);
            if (progress != null)
            {
                InitLength(length);
                InitCallback(progress);
            }
        }


        private long length = -1;

        private void InitStream(Stream stream)
        {
            this.stream = stream;
        }
        private void InitLength(long? length)
        {
            if (length != null)
            {
                this.length = length.Value;
            }
            else
            {
                try
                {
                    this.length = stream.Length;
                }
                catch
                {
                }
            }
        }

        private void InitCallback(IProgress<SimpleProgress> progress)
        {
            if (length != -1 && length != 0)
            {
                this.callback = pos => progress.Report(new SimpleProgress((double)pos / length));
            }
            else
            {
                this.callback = pos => { };
            }
        }


        private void InitCallback(IProgress<DataTransferProgress> progress)
        {
            Stopwatch startTime = new Stopwatch();

            var lastReceivedData = TimeSpan.Zero;
            this.callback = pos =>
            {
                if (!startTime.IsRunning) startTime.Start();
                var speed = CalculateSpeedNow(pos, startTime, lastReceivedData);

                progress.Report(new DataTransferProgress(new FileSize(pos), length != -1 ? new FileSize(length) : (FileSize?)null, speed != null ? new FileSize(speed.Value) : FileSize.Zero));
                lastReceivedData = startTime.Elapsed;
            };

        }

        internal static long? CalculateSpeedNow(long downloadedBytes, Stopwatch sw, TimeSpan lastReceivedData)
        {
            if (sw == null) return null;

            var elapsed = sw.Elapsed;
            if (elapsed.TotalMilliseconds < 1000) return null;
            if (lastReceivedData != TimeSpan.Zero)
            {
                var diff = elapsed - lastReceivedData;
                if (diff.Ticks < 0 || diff.TotalMilliseconds > ZeroSpeedAfterSilenceMilliseconds) return 0;
            }
            return (long)(downloadedBytes / elapsed.TotalSeconds);
        }

        private const int ZeroSpeedAfterSilenceMilliseconds = 1000;


        public StreamWithProgressCallback(Stream stream, Action<long> callback)
        {
            this.stream = stream;
            this.callback = callback;
        }

        public StreamWithProgressCallback(Stream stream, Action<long> callback, Action onDispose)
        {
            this.stream = stream;
            this.callback = callback;
            this.onDispose = onDispose;
        }

        public override bool CanRead
        {
            get { return stream.CanRead; }
        }

        public override bool CanSeek
        {
            get { return stream.CanSeek; }
        }

        public override bool CanWrite
        {
            get { return stream.CanWrite; }
        }

        public override void Flush()
        {
            stream.Flush();
        }

        public override long Length
        {
            get { return stream.Length; }
        }

        public override long Position
        {
            get
            {
                return stream.Position;
            }
            set
            {
                stream.Position = value;
                this.position = value;
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var c = stream.Read(buffer, offset, count);
            if (c != -1) position += c;
            callback(position);
            return c;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            var s = stream.Seek(offset, origin);
            position = s;
            return s;
        }

        public override void SetLength(long value)
        {
            stream.SetLength(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            stream.Write(buffer, offset, count);
            position += count;
            callback(position);
        }

        public bool LeaveOpen { get; set; }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (onDispose != null) onDispose();
                onDispose = null;
                if (!LeaveOpen)
                    stream.Dispose();
            }
        }


        public override int ReadByte()
        {
            var b = stream.ReadByte();
            if(b != -1) position++;
            return b;
        }

        public override void WriteByte(byte value)
        {
            stream.WriteByte(value);
            position++;
        }
    }
}
