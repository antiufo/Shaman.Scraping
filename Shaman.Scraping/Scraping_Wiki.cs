using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shaman.Scraping
{
    public class WikiScraper : WebsiteScraper
    {
        public Uri Home { get; set;}
    
        protected override void Initialize()
        {
            var root = Home;
            DestinationSuggestedName = root.Host.TrimStart("www.");


            var downloadSources = false;
            var downloadRecentPageHistory = false;
            var downloadNoRedirects = false;
            var downloadUsers = false;

            this.ShouldScrape = (url, prerequisite) =>
            {
                if (url.IsHostedOn(root.Host))
                {
                    if (url == root) return true;
                    //if (url.AbsolutePath.StartsWith("/images/")) return false;
                    if (prerequisite) return true;

                    if (url.Host != root.Host || url.Scheme != root.Scheme) return false;
                    string pageName = null;
                    if (url.GetQueryParameter("action") == "edit")
                    {

                    }
                    if (url.HasExactlyQueryParameters("title", "action"))
                    {
                        var action = url.GetQueryParameter("action");
                        if (action == "redirect") { if (!downloadNoRedirects) return false; }
                        else if (action == "edit") { if (!downloadSources) return false; }
                        else if (action == "history") { if(!downloadRecentPageHistory) return false; }
                        else return false;
                        pageName = url.GetQueryParameter("title");
                    }
                    else if (!url.HasNoQueryParameters()) return false;

                    if (pageName == null)
                    {
                        pageName = url.AbsolutePath.TryCaptureAfter("/wiki/");
                    }
                    if (pageName != null)
                    {
                        if (pageName.StartsWith("Speciale:PuntanoQui")) return false;
                        if (pageName.StartsWith("Speciale:ModificheCorrelate")) return false;
                        if (pageName.StartsWith("Speciale:RicercaISBN")) return false;
                        if (pageName.StartsWith("Speciale:Registri/")) return false;

                        if (pageName.StartsWith("Special:WhatLinksHere")) return false;
                        if (pageName.StartsWith("Special:RecentChangesLinked")) return false;
                        if (pageName.StartsWith("Special:Log/")) return false;
                        if (pageName.StartsWith("Special:AbuseLog/")) return false;
                        if (pageName.StartsWith("Special:AbuseFilter/")) return false;

                        if (!downloadUsers)
                        {
                            if (pageName.StartsWith("User:")) return false;
                            if (pageName.StartsWith("Utente:")) return false;
                        }

                        if (pageName.StartsWith("File:")) return false;
                        if (pageName.StartsWith("Speciale:Contributi/")) return false;
                        if (pageName.StartsWith("Special:Contributions/")) return false;
                        if (pageName.StartsWith("Talk:")) return false;
                        if (pageName.StartsWith("Essay_talk:")) return false;
                        if (pageName.StartsWith("User_talk:")) return false;
                        if (pageName.StartsWith("Fun_talk:")) return false;
                        if (pageName.StartsWith("Template_talk:")) return false;
                        if (pageName.StartsWith("Category_talk:")) return false;
                        if (pageName.StartsWith("Special:BookSources")) return false;
                        if (pageName.StartsWith("RationalWiki:To_do_list/todo")) return false;
                        if (pageName.StartsWith("cz")) return false;
                        if (pageName.StartsWith("KTKT")) return false;
                        return true;
                    }
                    return false;
                }


                if (prerequisite) return null;
                return false;
            };

            this.AddToCrawl(root);
            this.ReconsiderSkippedUrls();
        }
    }
}
