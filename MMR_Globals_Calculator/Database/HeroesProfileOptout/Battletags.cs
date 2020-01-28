using System;
using System.Collections.Generic;

namespace MMR_Globals_Calculator.Database.HeroesProfileOptout
{
    public partial class Battletags
    {
        public uint PlayerId { get; set; }
        public int BlizzId { get; set; }
        public string Battletag { get; set; }
        public sbyte Region { get; set; }
    }
}
