using System;
using System.Collections.Generic;

namespace MMR_Globals_Calculator.Database.HeroesProfile
{
    public partial class ReplayDraftOrder
    {
        public int ReplayId { get; set; }
        public string Type { get; set; }
        public int PickId { get; set; }
        public int Hero { get; set; }
    }
}
