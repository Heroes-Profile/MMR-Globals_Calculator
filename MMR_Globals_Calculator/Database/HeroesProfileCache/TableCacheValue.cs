using System;
using System.Collections.Generic;

namespace MMR_Globals_Calculator.Database.HeroesProfileCache
{
    public partial class TableCacheValue
    {
        public string TableToCache { get; set; }
        public int CacheNumber { get; set; }
        public DateTime DateCached { get; set; }
    }
}
