using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shaman.Runtime
{
    public static partial class Conversions
    {
        [StaticFieldCategory(StaticFieldCategory.Stable)]
        private readonly static CultureSpecificConverter EnglishConverter = new CultureSpecificConverter()
        {
            Ago = "ago",
            At = "at",
            Today = "today",
            Yesterday = "yesterday",
            Tomorrow = "tomorrow",
            DaySuffixes = "st|nd|rd|th",
            SinglrTimeIntervals = new[] { "year|yr", "month", "week|wk", "day", "hour|hr", "minute|min", "second|sec" },
            PluralTimeIntervals = new[] { "years|yrs", "months", "weeks|wks", "days", "hours|hrs", "minutes|mins", "seconds|secs" },
            WordsToStrip = new[] { "posted", "on", "submitted", "about", "and", "last", "edited", "created", "modified", "posted" }
        };
        [StaticFieldCategory(StaticFieldCategory.Stable)]
        private readonly static CultureSpecificConverter SwedishConverter = new CultureSpecificConverter()
        {
            Ago = "sedan",
            At = "kl",
            Today = "idag",
            Yesterday = "igår",
            Tomorrow = "i morgon",
            SinglrTimeIntervals = new[] { "år", "månad", "vecka", "dag", "timme", "minut", "sekund" },
            PluralTimeIntervals = new[] { "år", "månader", "veckor", "dagar", "timmar", "minuter", "sekunder" },
            WordsToStrip = new[] { "upplagt", "postade", "och" },
        };
        [StaticFieldCategory(StaticFieldCategory.Stable)]
        private readonly static CultureSpecificConverter DanishConverter = new CultureSpecificConverter()
        {
            Ago = "siden",
            At = "kl",
            Today = "i dag",
            Yesterday = "i går",
            Tomorrow = "i morgen",
            SinglrTimeIntervals = new[] { "år", "måned", "uge", "dag", "time", "minut", "sekund" },
            PluralTimeIntervals = new[] { "år", "måneder", "uger", "dage", "timer", "minutter", "sekunder" },
            WordsToStrip = new[] { "udstationeret", "og" },
        };

        [StaticFieldCategory(StaticFieldCategory.Stable)]
        private readonly static CultureSpecificConverter DutchConverter = new CultureSpecificConverter()
        {
            Ago = "geleden",
            At = "om",
            Today = "vandaag",
            Yesterday = "gisteren",
            Tomorrow = "morgen",
            SinglrTimeIntervals = new[] { "jaar", "maand", "week", "dag", "uur", "minuut", "seconde" },
            PluralTimeIntervals = new[] { "jaar", "maanden", "weken", "dagen", "uur", "minuten", "seconden" },
            WordsToStrip = new[] { "gepost", "en", "geplaatst" },
        };

        [StaticFieldCategory(StaticFieldCategory.Stable)]
        private readonly static CultureSpecificConverter ItalianConverter = new CultureSpecificConverter()
        {
            Ago = "fa",
            At = "alle",
            Today = "oggi",
            Yesterday = "ieri",
            Tomorrow = "domani",
            SinglrTimeIntervals = new[] { "anno", "mese", "settimana", "giorno", "ora", "minuto", "secondo" },
            PluralTimeIntervals = new[] { "anni", "mesi", "settimane", "giorni", "ore", "minuti", "secondi" },
            WordsToStrip = new[] { "postato", "aggiunto", "circa", "e" },
        };
        [StaticFieldCategory(StaticFieldCategory.Stable)]
        private readonly static CultureSpecificConverter SpanishConverter = new CultureSpecificConverter()
        {
            Ago = "hace",
            At = "a las",
            Today = "hoy",
            Yesterday = "ayer",
            Tomorrow = "mañana",
            SinglrTimeIntervals = new[] { "año", "mes", "semana", "día", "hora", "minuto", "segundo" },
            PluralTimeIntervals = new[] { "años", "meses", "semanas", "días", "horas", "minutos", "segundos" },
            WordsToStrip = new[] { "publicado", "añadido", "y" },
        };
        [StaticFieldCategory(StaticFieldCategory.Stable)]
        private readonly static CultureSpecificConverter GermanConverter = new CultureSpecificConverter()
        {
            Ago = "vor",
            At = "um",
            Today = "heute",
            Yesterday = "gestern",
            Tomorrow = "morgen",
            SinglrTimeIntervals = new[] { "jahr", "monat", "woche", "tag", "stunde", "minute", "sekunde" },
            PluralTimeIntervals = new[] { "jahre", "monate", "wochen", "tage", "stunden", "minuten", "sekunden" },
            WordsToStrip = new[] { "veröffentlicht", "geposted", "hinzugefügt", "und" },
        };

    }
}
