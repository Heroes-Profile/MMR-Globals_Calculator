using System;
using System.Collections.Generic;

namespace MMR_Globals_Calculator.Database.HeroesProfile
{
    public partial class GlobalHeroMatchupsEnemy
    {
        public string GameVersion { get; set; }
        public sbyte GameType { get; set; }
        public sbyte LeagueTier { get; set; }
        public sbyte HeroLeagueTier { get; set; }
        public sbyte RoleLeagueTier { get; set; }
        public sbyte GameMap { get; set; }
        public uint HeroLevel { get; set; }
        public sbyte Hero { get; set; }
        public sbyte Enemy { get; set; }
        public sbyte Mirror { get; set; }
        public int Region { get; set; }
        public sbyte WinLoss { get; set; }
        public uint GamesPlayed { get; set; }
    }
}
