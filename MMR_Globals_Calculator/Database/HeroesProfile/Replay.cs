using System;
using System.Collections.Generic;

namespace MMR_Globals_Calculator.Database.HeroesProfile
{
    public partial class Replay
    {
        public uint ReplayId { get; set; }
        public int? ParsedId { get; set; }
        public byte GameType { get; set; }
        public DateTime GameDate { get; set; }
        public ushort GameLength { get; set; }
        public byte GameMap { get; set; }
        public string GameVersion { get; set; }
        public byte Region { get; set; }
        public DateTime DateAdded { get; set; }
        public sbyte? MmrRan { get; set; }
    }
}
