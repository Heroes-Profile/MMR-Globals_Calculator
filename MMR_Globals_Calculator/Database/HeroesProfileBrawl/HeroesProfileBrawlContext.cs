using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace MMR_Globals_Calculator.Database.HeroesProfileBrawl
{
    public partial class HeroesProfileBrawlContext : DbContext
    {
        public HeroesProfileBrawlContext()
        {
        }

        public HeroesProfileBrawlContext(DbContextOptions<HeroesProfileBrawlContext> options)
            : base(options)
        {
        }

        public virtual DbSet<Player> Player { get; set; }
        public virtual DbSet<Replay> Replay { get; set; }
        public virtual DbSet<Scores> Scores { get; set; }
        public virtual DbSet<Talents> Talents { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
#warning To protect potentially sensitive information in your connection string, you should move it out of source code. See http://go.microsoft.com/fwlink/?LinkId=723263 for guidance on storing connection strings.
                optionsBuilder.UseMySql("server=localhost;port=3306;user=root;database=heroesprofile_brawl", x => x.ServerVersion("10.4.10-mariadb"));
            }
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Player>(entity =>
            {
                entity.HasKey(e => new { e.ReplayId, e.Battletag, e.Hero })
                    .HasName("PRIMARY");

                entity.ToTable("player");

                entity.HasIndex(e => new { e.BlizzId, e.Hero })
                    .HasName("heroesprofile_brawl_player_blizz_id_hero_index");

                entity.HasIndex(e => new { e.ReplayId, e.BlizzId, e.Hero })
                    .HasName("heroesprofile_brawl_player_replayid_blizz_id_hero_index");

                entity.Property(e => e.ReplayId)
                    .HasColumnName("replayID")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Battletag)
                    .HasColumnName("battletag")
                    .HasColumnType("varchar(50)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.Hero)
                    .HasColumnName("hero")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.BlizzId)
                    .HasColumnName("blizz_id")
                    .HasColumnType("int(11)");

                entity.Property(e => e.HeroLevel)
                    .HasColumnName("hero_level")
                    .HasColumnType("smallint(6)");

                entity.Property(e => e.MasteryTaunt)
                    .HasColumnName("mastery_taunt")
                    .HasColumnType("smallint(6)");

                entity.Property(e => e.Party)
                    .IsRequired()
                    .HasColumnName("party")
                    .HasColumnType("varchar(45)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.Team)
                    .HasColumnName("team")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.Winner)
                    .HasColumnName("winner")
                    .HasColumnType("tinyint(4)");
            });

            modelBuilder.Entity<Replay>(entity =>
            {
                entity.ToTable("replay");

                entity.HasIndex(e => e.GameDate)
                    .HasName("heroesprofile_brawl_replay_game_date_index");

                entity.HasIndex(e => e.GlobalsRan)
                    .HasName("heroesprofile_brawl_replay_globals_ran_index");

                entity.HasIndex(e => e.Region)
                    .HasName("heroesprofile_brawl_replay_region_index");

                entity.HasIndex(e => new { e.ReplayId, e.GameDate })
                    .HasName("heroesprofile_brawl_replay_replayid_game_date_index");

                entity.HasIndex(e => new { e.ReplayId, e.Region, e.GameDate })
                    .HasName("heroesprofile_brawl_replay_replayid_region_game_date_index");

                entity.Property(e => e.ReplayId)
                    .HasColumnName("replayID")
                    .HasColumnType("int(11)");

                entity.Property(e => e.DateAdded)
                    .HasColumnName("date_added")
                    .HasColumnType("datetime");

                entity.Property(e => e.GameDate)
                    .HasColumnName("game_date")
                    .HasColumnType("datetime");

                entity.Property(e => e.GameLength)
                    .HasColumnName("game_length")
                    .HasColumnType("smallint(6)");

                entity.Property(e => e.GameMap)
                    .HasColumnName("game_map")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.GameVersion)
                    .IsRequired()
                    .HasColumnName("game_version")
                    .HasColumnType("varchar(32)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.GlobalsRan)
                    .HasColumnName("globals_ran")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.Region)
                    .HasColumnName("region")
                    .HasColumnType("tinyint(4)");
            });

            modelBuilder.Entity<Scores>(entity =>
            {
                entity.HasKey(e => new { e.ReplayId, e.Battletag })
                    .HasName("PRIMARY");

                entity.ToTable("scores");

                entity.Property(e => e.ReplayId)
                    .HasColumnName("replayID")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Battletag)
                    .HasColumnName("battletag")
                    .HasColumnType("varchar(50)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.Assists)
                    .HasColumnName("assists")
                    .HasColumnType("int(11)");

                entity.Property(e => e.ClutchHeals)
                    .HasColumnName("clutch_heals")
                    .HasColumnType("int(11)");

                entity.Property(e => e.CreepDamage)
                    .HasColumnName("creep_damage")
                    .HasColumnType("int(11)");

                entity.Property(e => e.DamageTaken)
                    .HasColumnName("damage_taken")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Deaths)
                    .HasColumnName("deaths")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Escapes)
                    .HasColumnName("escapes")
                    .HasColumnType("int(11)");

                entity.Property(e => e.ExperienceContribution)
                    .HasColumnName("experience_contribution")
                    .HasColumnType("int(11)");

                entity.Property(e => e.FirstToTen)
                    .HasColumnName("first_to_ten")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Healing)
                    .HasColumnName("healing")
                    .HasColumnType("int(11)");

                entity.Property(e => e.HeroDamage)
                    .HasColumnName("hero_damage")
                    .HasColumnType("int(11)");

                entity.Property(e => e.HighestKillStreak)
                    .HasColumnName("highest_kill_streak")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Kills)
                    .HasColumnName("kills")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Level)
                    .HasColumnName("level")
                    .HasColumnType("int(11)");

                entity.Property(e => e.MatchAward)
                    .HasColumnName("match_award")
                    .HasColumnType("int(11)");

                entity.Property(e => e.MercCampCaptures)
                    .HasColumnName("merc_camp_captures")
                    .HasColumnType("int(11)");

                entity.Property(e => e.MetaExperience)
                    .HasColumnName("meta_experience")
                    .HasColumnType("int(11)");

                entity.Property(e => e.MinionDamage)
                    .HasColumnName("minion_damage")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Multikill)
                    .HasColumnName("multikill")
                    .HasColumnType("int(11)");

                entity.Property(e => e.OutnumberedDeaths)
                    .HasColumnName("outnumbered_deaths")
                    .HasColumnType("int(11)");

                entity.Property(e => e.PhysicalDamage)
                    .HasColumnName("physical_damage")
                    .HasColumnType("int(11)");

                entity.Property(e => e.ProtectionAllies)
                    .HasColumnName("protection_allies")
                    .HasColumnType("int(11)");

                entity.Property(e => e.RegenGlobes)
                    .HasColumnName("regen_globes")
                    .HasColumnType("int(11)");

                entity.Property(e => e.RootingEnemies)
                    .HasColumnName("rooting_enemies")
                    .HasColumnType("int(11)");

                entity.Property(e => e.SelfHealing)
                    .HasColumnName("self_healing")
                    .HasColumnType("int(11)");

                entity.Property(e => e.SiegeDamage)
                    .HasColumnName("siege_damage")
                    .HasColumnType("int(11)");

                entity.Property(e => e.SilencingEnemies)
                    .HasColumnName("silencing_enemies")
                    .HasColumnType("int(11)");

                entity.Property(e => e.SpellDamage)
                    .HasColumnName("spell_damage")
                    .HasColumnType("int(11)");

                entity.Property(e => e.StructureDamage)
                    .HasColumnName("structure_damage")
                    .HasColumnType("int(11)");

                entity.Property(e => e.StunningEnemies)
                    .HasColumnName("stunning_enemies")
                    .HasColumnType("int(11)");

                entity.Property(e => e.SummonDamage)
                    .HasColumnName("summon_damage")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Takedowns)
                    .HasColumnName("takedowns")
                    .HasColumnType("int(11)");

                entity.Property(e => e.TeamfightDamageTaken)
                    .HasColumnName("teamfight_damage_taken")
                    .HasColumnType("int(11)");

                entity.Property(e => e.TeamfightEscapes)
                    .HasColumnName("teamfight_escapes")
                    .HasColumnType("int(11)");

                entity.Property(e => e.TeamfightHealing)
                    .HasColumnName("teamfight_healing")
                    .HasColumnType("int(11)");

                entity.Property(e => e.TeamfightHeroDamage)
                    .HasColumnName("teamfight_hero_damage")
                    .HasColumnType("int(11)");

                entity.Property(e => e.TimeCcEnemyHeroes)
                    .HasColumnName("time_cc_enemy_heroes")
                    .HasColumnType("int(11)");

                entity.Property(e => e.TimeSpentDead)
                    .HasColumnName("time_spent_dead")
                    .HasColumnType("int(11)");

                entity.Property(e => e.TownKills)
                    .HasColumnName("town_kills")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Vengeance)
                    .HasColumnName("vengeance")
                    .HasColumnType("int(11)");

                entity.Property(e => e.WatchTowerCaptures)
                    .HasColumnName("watch_tower_captures")
                    .HasColumnType("int(11)");
            });

            modelBuilder.Entity<Talents>(entity =>
            {
                entity.HasKey(e => new { e.ReplayId, e.Battletag })
                    .HasName("PRIMARY");

                entity.ToTable("talents");

                entity.Property(e => e.ReplayId)
                    .HasColumnName("replayID")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Battletag)
                    .HasColumnName("battletag")
                    .HasColumnType("varchar(50)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.LevelFour)
                    .HasColumnName("level_four")
                    .HasColumnType("int(11)");

                entity.Property(e => e.LevelOne)
                    .HasColumnName("level_one")
                    .HasColumnType("int(11)");

                entity.Property(e => e.LevelSeven)
                    .HasColumnName("level_seven")
                    .HasColumnType("int(11)");

                entity.Property(e => e.LevelSixteen)
                    .HasColumnName("level_sixteen")
                    .HasColumnType("int(11)");

                entity.Property(e => e.LevelTen)
                    .HasColumnName("level_ten")
                    .HasColumnType("int(11)");

                entity.Property(e => e.LevelThirteen)
                    .HasColumnName("level_thirteen")
                    .HasColumnType("int(11)");

                entity.Property(e => e.LevelTwenty)
                    .HasColumnName("level_twenty")
                    .HasColumnType("int(11)");
            });

            OnModelCreatingPartial(modelBuilder);
        }

        partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
    }
}
