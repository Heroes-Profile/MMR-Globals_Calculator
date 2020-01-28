using System;
using System.Collections.Generic;

namespace MMR_Globals_Calculator.Database.HeroesProfile
{
    public partial class SeasonGameVersions
    {
        public int Season { get; set; }
        public string GameVersion { get; set; }
        public DateTime? DateAdded { get; set; }
    }
}
