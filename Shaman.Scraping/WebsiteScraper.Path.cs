using Shaman.Runtime;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shaman.Scraping
{
    public partial class WebsiteScraper
    {

        internal static string GetRelativePath(string basePath, string path)
        {
            Uri pathUri = new Uri(path);
            //// Folders must end in a slash
            //if (!basePath.EndsWith(Path.DirectorySeparatorChar.ToString()))
            //{
            //    basePath += Path.DirectorySeparatorChar;
            //}
            Uri folderUri = new Uri(basePath);
            return Uri.UnescapeDataString(folderUri.MakeRelativeUri(pathUri).ToString());
        }


        public static string GetPathInternal(string root, Uri url, string contentType = null, int pathComponentsToKeepAtRoot = -1)
        {

            var sb = ReseekableStringBuilder.AcquirePooledStringBuilder();
            if (root != null)
            {
                sb.Append(root);
                sb.Replace('\\', '/');
                if (sb[sb.Length - 1] != '/') sb.Append('/');
            }
            sb.Append(url.Authority.Replace(":", "_"));
            sb.Append('/');

            
            var pathComponents = url.AbsolutePath.SplitFast('/', StringSplitOptions.RemoveEmptyEntries);



            if (pathComponentsToKeepAtRoot != -1)
            {
                if (pathComponentsToKeepAtRoot < pathComponents.Length)
                {
                    var last = string.Join("-", pathComponents.Skip(pathComponentsToKeepAtRoot));
                    pathComponents = pathComponents.Take(pathComponentsToKeepAtRoot).Concat(new[] { last }).ToArray();
                }
            }


            if (pathComponents.Length == 0 && string.IsNullOrEmpty(url.Query))
            {
                sb.Append("index");
            }
            else
            {
                for (int i = 0; i < pathComponents.Length; i++)
                {
                    if (i != 0) sb.Append('/');
                    var v = Uri.UnescapeDataString(pathComponents[i].Trim().Replace('+', ' '));
                    if (v == "..") v = "،،";

                    AppendEscaped(sb, v);
                }

                if (!string.IsNullOrEmpty(url.Query))
                {
                    AppendEscaped(sb, url.Query);
                }
                if (!string.IsNullOrEmpty(url.Fragment))
                {
                    AppendEscaped(sb, url.Fragment);
                }
            }

            string extension = null;

            var ext = GetExtension(url);

            if (ext != null)
            {
                if (Configuration_KnownExtensions.Contains(ext))
                {
                    extension = ext;
                }
            }

            if (contentType != null)
            {
                if (contentType.Contains("/html")) extension = null;
                else if (extension == null)
                {
                    extension = MimeToExtension(contentType);
                }
            }

            if (extension == null) extension = ".html";

            if (!(
                EndsWith(sb, extension) || (extension == ".html" && EndsWith(sb, ".htm"))
            ))
                sb.Append(extension);

            return ReseekableStringBuilder.GetValueAndRelease(sb);
        }

        private static string MimeToExtension(string contentType)
        {
            switch (contentType)
            {
                case "image/jpeg": return ".jpg";
                case "image/gif": return ".gif";
                case "image/png": return ".png";
                case "video/webm": return ".webm";
                case "video/mp4": return ".mp4";
                case "video/mpg": return ".mpeg";
                case "video/mpeg": return ".mpeg";
                case "audio/mpeg": return ".mp3";
                case "text/json": return ".json";
                case "application/json": return ".json";
                case "application/javascript": return ".js";
                case "application/x-javascript": return ".js";
                case "text/javascript": return ".js";
                case "text/css": return ".css";
                case "application/xml": return ".xml";
                case "text/plain": return ".txt";
                case "text/html": return ".html";
                default: return null;
            }
        }

        public static string GetExtension(Uri url)
        {
            var absolutePath = url.AbsolutePath;
            var lastSlash = absolutePath.LastIndexOf('/');
            var name = absolutePath.Substring(lastSlash + 1);
            var dot = name.LastIndexOf('.');
            if (dot != -1) return name.Substring(dot).ToLowerFast();
            return null;
        }

        private static readonly string HexUpperChars = "0123456789ABCDEF";

        private readonly static char[] InvalidFileNameChars =
            Path.GetInvalidFileNameChars().Concat("‽∯∺℅%℆،".ToArray()).ToArray();
        private static string[] Configuration_KnownExtensions = new[] {
            ".jpg", ".jpeg", ".png", ".gif", ".webp", ".webm", ".svg", ".css", ".js", ".mp3", ".mp4", ".mov", ".3gp", ".wmf", ".amr",
            ".tgz", ".iso", ".tif", ".tiff", ".xps", ".vhd", ".log",
            ".zip", ".exe", ".msi", ".mpg", ".mpeg", ".wmv", ".avi", ".ogg", ".woff", ".eot", ".otf", ".ttf", ".swf", ".mid",
            ".wav", ".xml", ".rss", ".txt", ".ico", ".rar", ".7z", ".gz", ".tar", ".iso", ".pdf", ".flv",
            ".ppt", ".pptx", ".doc", ".docx", ".rtf", ".xls", ".xlsx", ".pps", ".jpe", ".bmp", ".odt" };
        private static void AppendEscaped(StringBuilder sb, string v)
        {
            for (int i = 0; i < v.Length; i++)
            {
                var ch = v[i];
                if (InvalidFileNameChars.Contains(ch))
                {
                    if (ch == '?') sb.Append('‽');
                    else if (ch == '/') sb.Append('∯');
                    else if (ch == ':') sb.Append('∺');
                    else if (ch == '%') sb.Append('℅');
                    else
                    {
                        sb.Append('℆');
                        sb.Append(HexUpperChars[(ch & 0xf0) >> 4]);
                        sb.Append(HexUpperChars[ch & 0xf]);
                    }
                }
                else
                {
                    sb.Append(ch);
                }
            }
        }


        private static bool EndsWith(StringBuilder _string, string str)
        {
            bool result;
            if (_string == null) result = str.Length == 0;
            else if (str.Length > _string.Length) result = false;
            else
            {
                result = true;
                var offset = (_string.Length - str.Length);
                for (int i = str.Length - 1; i >= 0; i--)
                {
                    if (_string[offset + i] != str[i])
                    {
                        result = false;
                        break;
                    }
                }
            }
            return result;
        }


    }
}
