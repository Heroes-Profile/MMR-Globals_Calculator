using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace MMR_Globals_Calculator.Database.HeroesProfileOptout
{
    public partial class HeroesProfileOptoutContext : DbContext
    {
        public HeroesProfileOptoutContext()
        {
        }

        public HeroesProfileOptoutContext(DbContextOptions<HeroesProfileOptoutContext> options)
            : base(options)
        {
        }

        public virtual DbSet<Battletags> Battletags { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
#warning To protect potentially sensitive information in your connection string, you should move it out of source code. See http://go.microsoft.com/fwlink/?LinkId=723263 for guidance on storing connection strings.
                optionsBuilder.UseMySql("server=localhost;port=3306;user=root;database=heroesprofile_optout", x => x.ServerVersion("10.4.10-mariadb"));
            }
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Battletags>(entity =>
            {
                entity.HasNoKey();

                entity.ToTable("battletags");

                entity.HasIndex(e => new { e.BlizzId, e.Battletag, e.Region })
                    .HasName("heroesprofile_optout_battletags_blizz_id_battletag_region_unique")
                    .IsUnique();

                entity.Property(e => e.Battletag)
                    .IsRequired()
                    .HasColumnName("battletag")
                    .HasColumnType("varchar(45)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.BlizzId)
                    .HasColumnName("blizz_id")
                    .HasColumnType("int(11)");

                entity.Property(e => e.PlayerId)
                    .HasColumnName("player_id")
                    .HasColumnType("int(10) unsigned");

                entity.Property(e => e.Region)
                    .HasColumnName("region")
                    .HasColumnType("tinyint(4)");
            });

            OnModelCreatingPartial(modelBuilder);
        }

        partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
    }
}
