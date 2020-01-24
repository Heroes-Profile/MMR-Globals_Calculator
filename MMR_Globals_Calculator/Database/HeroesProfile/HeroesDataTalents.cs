using System;
using System.Collections.Generic;

namespace MMR_Globals_Calculator.Database.HeroesProfile
{
    public partial class HeroesDataTalents
    {
        public uint TalentId { get; set; }
        public string HeroName { get; set; }
        public string ShortName { get; set; }
        public string AttributeId { get; set; }
        public string Title { get; set; }
        public string TalentName { get; set; }
        public string Description { get; set; }
        public string Status { get; set; }
        public string Hotkey { get; set; }
        public string Cooldown { get; set; }
        public string ManaCost { get; set; }
        public string Sort { get; set; }
        public int Level { get; set; }
        public string Icon { get; set; }
        public int? RequiredTalentId { get; set; }
    }
}
