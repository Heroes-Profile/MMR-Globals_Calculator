using System;
using System.Collections.Generic;

namespace MMR_Globals_Calculator.Database.HeroesProfile
{
    public partial class Heroes
    {
        public uint Id { get; set; }
        public string Name { get; set; }
        public string ShortName { get; set; }
        public string AltName { get; set; }
        public string Role { get; set; }
        public string NewRole { get; set; }
        public string Type { get; set; }
        public DateTime? ReleaseDate { get; set; }
        public DateTime? ReworkDate { get; set; }
        public string AttributeId { get; set; }
    }
}
