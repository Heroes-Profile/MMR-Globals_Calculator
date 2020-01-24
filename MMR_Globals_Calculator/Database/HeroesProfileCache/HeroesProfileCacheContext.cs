using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace MMR_Globals_Calculator.Database.HeroesProfileCache
{
    public partial class HeroesProfileCacheContext : DbContext
    {
        public HeroesProfileCacheContext()
        {
        }

        public HeroesProfileCacheContext(DbContextOptions<HeroesProfileCacheContext> options)
            : base(options)
        {
        }

        public virtual DbSet<Cache> Cache { get; set; }
        public virtual DbSet<GlobalHeroChange> GlobalHeroChange { get; set; }
        public virtual DbSet<Leaderboard> Leaderboard { get; set; }
        public virtual DbSet<Sessions> Sessions { get; set; }
        public virtual DbSet<TableCacheValue> TableCacheValue { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
#warning To protect potentially sensitive information in your connection string, you should move it out of source code. See http://go.microsoft.com/fwlink/?LinkId=723263 for guidance on storing connection strings.
                optionsBuilder.UseMySql("server=localhost;port=3306;user=root;database=heroesprofile_cache", x => x.ServerVersion("10.4.10-mariadb"));
            }
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Cache>(entity =>
            {
                entity.HasNoKey();

                entity.ToTable("cache");

                entity.HasIndex(e => e.Key)
                    .HasName("heroesprofile_cache_cache_key_unique")
                    .IsUnique();

                entity.Property(e => e.Expiration)
                    .HasColumnName("expiration")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Key)
                    .IsRequired()
                    .HasColumnName("key")
                    .HasColumnType("varchar(50)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.Value)
                    .IsRequired()
                    .HasColumnName("value")
                    .HasColumnType("longtext")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");
            });

            modelBuilder.Entity<GlobalHeroChange>(entity =>
            {
                entity.HasKey(e => new { e.GameVersion, e.GameType, e.Hero })
                    .HasName("PRIMARY");

                entity.ToTable("global_hero_change");

                entity.Property(e => e.GameVersion)
                    .HasColumnName("game_version")
                    .HasColumnType("varchar(45)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.GameType)
                    .HasColumnName("game_type")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.Hero)
                    .HasColumnName("hero")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.BanRate).HasColumnName("ban_rate");

                entity.Property(e => e.Bans)
                    .HasColumnName("bans")
                    .HasColumnType("int(11)");

                entity.Property(e => e.GamesPlayed)
                    .HasColumnName("games_played")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Losses)
                    .HasColumnName("losses")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Popularity).HasColumnName("popularity");

                entity.Property(e => e.WinRate).HasColumnName("win_rate");

                entity.Property(e => e.Wins)
                    .HasColumnName("wins")
                    .HasColumnType("int(11)");
            });

            modelBuilder.Entity<Leaderboard>(entity =>
            {
                entity.HasKey(e => new { e.GameType, e.Season, e.Type, e.Rank, e.CacheNumber })
                    .HasName("PRIMARY");

                entity.ToTable("leaderboard");

                entity.HasIndex(e => new { e.GameType, e.Season, e.Type, e.CacheNumber })
                    .HasName("Index 1");

                entity.HasIndex(e => new { e.GameType, e.Season, e.Type, e.CacheNumber, e.Region })
                    .HasName("Index 2");

                entity.Property(e => e.GameType)
                    .HasColumnName("game_type")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Season)
                    .HasColumnName("season")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Type)
                    .HasColumnName("type")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Rank)
                    .HasColumnName("rank")
                    .HasColumnType("int(11)");

                entity.Property(e => e.CacheNumber)
                    .HasColumnName("cache_number")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Battletag)
                    .IsRequired()
                    .HasColumnName("battletag")
                    .HasColumnType("varchar(255)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.BlizzId)
                    .HasColumnName("blizz_id")
                    .HasColumnType("int(11)");

                entity.Property(e => e.ConservativeRating).HasColumnName("conservative_rating");

                entity.Property(e => e.GamesPlayed)
                    .HasColumnName("games_played")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Loss)
                    .HasColumnName("loss")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Rating).HasColumnName("rating");

                entity.Property(e => e.Region)
                    .HasColumnName("region")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.SplitBattletag)
                    .IsRequired()
                    .HasColumnName("split_battletag")
                    .HasColumnType("varchar(255)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.Win)
                    .HasColumnName("win")
                    .HasColumnType("int(11)");

                entity.Property(e => e.WinRate).HasColumnName("win_rate");
            });

            modelBuilder.Entity<Sessions>(entity =>
            {
                entity.HasNoKey();

                entity.ToTable("sessions");

                entity.HasIndex(e => e.Id)
                    .HasName("heroesprofile_cache_sessions_id_unique")
                    .IsUnique();

                entity.Property(e => e.Id)
                    .IsRequired()
                    .HasColumnName("id")
                    .HasColumnType("varchar(50)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.IpAddress)
                    .HasColumnName("ip_address")
                    .HasColumnType("varchar(45)")
                    .HasDefaultValueSql("'NULL'")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.LastActivity)
                    .HasColumnName("last_activity")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Payload)
                    .IsRequired()
                    .HasColumnName("payload")
                    .HasColumnType("longtext")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.UserAgent)
                    .HasColumnName("user_agent")
                    .HasColumnType("text")
                    .HasDefaultValueSql("'NULL'")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.UserId)
                    .HasColumnName("user_id")
                    .HasColumnType("bigint(20) unsigned")
                    .HasDefaultValueSql("'NULL'");
            });

            modelBuilder.Entity<TableCacheValue>(entity =>
            {
                entity.HasKey(e => new { e.CacheNumber, e.TableToCache })
                    .HasName("PRIMARY");

                entity.ToTable("table_cache_value");

                entity.Property(e => e.CacheNumber)
                    .HasColumnName("cache_number")
                    .HasColumnType("int(11)");

                entity.Property(e => e.TableToCache)
                    .HasColumnName("table_to_cache")
                    .HasColumnType("varchar(45)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.DateCached)
                    .HasColumnName("date_cached")
                    .HasColumnType("datetime");
            });

            OnModelCreatingPartial(modelBuilder);
        }

        partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
    }
}
