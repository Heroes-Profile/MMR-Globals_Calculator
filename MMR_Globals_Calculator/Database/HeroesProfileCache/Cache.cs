using System;
using System.Collections.Generic;

namespace MMR_Globals_Calculator.Database.HeroesProfileCache
{
    public partial class Cache
    {
        public string Key { get; set; }
        public string Value { get; set; }
        public int Expiration { get; set; }
    }
}
