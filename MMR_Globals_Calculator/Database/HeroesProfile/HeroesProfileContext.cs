using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace MMR_Globals_Calculator.Database.HeroesProfile
{
    public partial class HeroesProfileContext : DbContext
    {
        public HeroesProfileContext()
        {
        }

        public HeroesProfileContext(DbContextOptions<HeroesProfileContext> options)
            : base(options)
        {
        }

        public virtual DbSet<BattlenetAccounts> BattlenetAccounts { get; set; }
        public virtual DbSet<Battletags> Battletags { get; set; }
        public virtual DbSet<GameTypes> GameTypes { get; set; }
        public virtual DbSet<GlobalHeroMatchupsAlly> GlobalHeroMatchupsAlly { get; set; }
        public virtual DbSet<GlobalHeroMatchupsEnemy> GlobalHeroMatchupsEnemy { get; set; }
        public virtual DbSet<GlobalHeroStats> GlobalHeroStats { get; set; }
        public virtual DbSet<GlobalHeroStatsBans> GlobalHeroStatsBans { get; set; }
        public virtual DbSet<GlobalHeroStatsOld> GlobalHeroStatsOld { get; set; }
        public virtual DbSet<GlobalHeroTalents> GlobalHeroTalents { get; set; }
        public virtual DbSet<GlobalHeroTalentsDetails> GlobalHeroTalentsDetails { get; set; }
        public virtual DbSet<Heroes> Heroes { get; set; }
        public virtual DbSet<HeroesDataAbilities> HeroesDataAbilities { get; set; }
        public virtual DbSet<HeroesDataTalents> HeroesDataTalents { get; set; }
        public virtual DbSet<HeroesTranslations> HeroesTranslations { get; set; }
        public virtual DbSet<LeagueBreakdowns> LeagueBreakdowns { get; set; }
        public virtual DbSet<LeagueTiers> LeagueTiers { get; set; }
        public virtual DbSet<Maps> Maps { get; set; }
        public virtual DbSet<MasterGamesPlayedData> MasterGamesPlayedData { get; set; }
        public virtual DbSet<MasterMmrData> MasterMmrData { get; set; }
        public virtual DbSet<Migrations> Migrations { get; set; }
        public virtual DbSet<MmrTypeIds> MmrTypeIds { get; set; }
        public virtual DbSet<Player> Player { get; set; }
        public virtual DbSet<Replay> Replay { get; set; }
        public virtual DbSet<ReplayBans> ReplayBans { get; set; }
        public virtual DbSet<ReplayDraftOrder> ReplayDraftOrder { get; set; }
        public virtual DbSet<ReplayExperienceBreakdown> ReplayExperienceBreakdown { get; set; }
        public virtual DbSet<Scores> Scores { get; set; }
        public virtual DbSet<SeasonDates> SeasonDates { get; set; }
        public virtual DbSet<SeasonGameVersions> SeasonGameVersions { get; set; }
        public virtual DbSet<TalentCombinations> TalentCombinations { get; set; }
        public virtual DbSet<Talents> Talents { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
#warning To protect potentially sensitive information in your connection string, you should move it out of source code. See http://go.microsoft.com/fwlink/?LinkId=723263 for guidance on storing connection strings.
                optionsBuilder.UseMySql("server=localhost;port=3306;user=root;database=heroesprofile", x => x.ServerVersion("10.4.10-mariadb"));
            }
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<BattlenetAccounts>(entity =>
            {
                entity.ToTable("battlenet_accounts");

                entity.Property(e => e.Id)
                    .HasColumnName("id")
                    .HasColumnType("bigint(20) unsigned");

                entity.Property(e => e.BattlenetAccessToken)
                    .IsRequired()
                    .HasColumnName("battlenet_access_token")
                    .HasColumnType("varchar(255)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.BattlenetId)
                    .HasColumnName("battlenet_id")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Battletag)
                    .IsRequired()
                    .HasColumnName("battletag")
                    .HasColumnType("varchar(255)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.CreatedAt)
                    .HasColumnName("created_at")
                    .HasColumnType("timestamp")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Region)
                    .HasColumnName("region")
                    .HasColumnType("int(11)");

                entity.Property(e => e.RememberToken)
                    .IsRequired()
                    .HasColumnName("remember_token")
                    .HasColumnType("varchar(255)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.UpdatedAt)
                    .HasColumnName("updated_at")
                    .HasColumnType("timestamp")
                    .HasDefaultValueSql("'NULL'");
            });

            modelBuilder.Entity<Battletags>(entity =>
            {
                entity.HasKey(e => e.PlayerId)
                    .HasName("PRIMARY");

                entity.ToTable("battletags");

                entity.HasIndex(e => e.AccountLevel)
                    .HasName("heroesprofile_battletags_account_level_index");

                entity.HasIndex(e => e.Battletag)
                    .HasName("heroesprofile_battletags_battletag_index");

                entity.HasIndex(e => e.OptOut)
                    .HasName("heroesprofile_battletags_opt_out_index");

                entity.HasIndex(e => e.Patreon)
                    .HasName("heroesprofile_battletags_patreon_index");

                entity.HasIndex(e => new { e.Battletag, e.Region })
                    .HasName("heroesprofile_battletags_battletag_region_index");

                entity.HasIndex(e => new { e.BlizzId, e.Battletag, e.Region })
                    .HasName("heroesprofile_battletags_blizz_id_battletag_region_unique")
                    .IsUnique();

                entity.Property(e => e.PlayerId)
                    .HasColumnName("player_id")
                    .HasColumnType("int(10) unsigned");

                entity.Property(e => e.AccountLevel)
                    .HasColumnName("account_level")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Battletag)
                    .IsRequired()
                    .HasColumnName("battletag")
                    .HasColumnType("varchar(45)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.BlizzId)
                    .HasColumnName("blizz_id")
                    .HasColumnType("int(11)");

                entity.Property(e => e.LatestGame)
                    .HasColumnName("latest_game")
                    .HasColumnType("datetime")
                    .HasDefaultValueSql("'''2014-06-26 13:13:34'''");

                entity.Property(e => e.OptOut)
                    .HasColumnName("opt_out")
                    .HasColumnType("tinyint(4)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Patreon)
                    .HasColumnName("patreon")
                    .HasColumnType("tinyint(4)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Region)
                    .HasColumnName("region")
                    .HasColumnType("tinyint(4)");
            });

            modelBuilder.Entity<GameTypes>(entity =>
            {
                entity.HasNoKey();

                entity.ToTable("game_types");

                entity.Property(e => e.Name)
                    .IsRequired()
                    .HasColumnName("name")
                    .HasColumnType("varchar(45)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.ShortName)
                    .IsRequired()
                    .HasColumnName("short_name")
                    .HasColumnType("varchar(45)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.TypeId)
                    .HasColumnName("type_id")
                    .HasColumnType("int(11)");
            });

            modelBuilder.Entity<GlobalHeroMatchupsAlly>(entity =>
            {
                entity.HasKey(e => new { e.GameVersion, e.GameType, e.LeagueTier, e.HeroLeagueTier, e.RoleLeagueTier, e.GameMap, e.HeroLevel, e.Hero, e.Ally, e.Mirror, e.WinLoss })
                    .HasName("PRIMARY");

                entity.ToTable("global_hero_matchups_ally");

                entity.HasIndex(e => new { e.GameVersion, e.GameType, e.Hero, e.LeagueTier, e.HeroLeagueTier, e.RoleLeagueTier, e.GameMap, e.HeroLevel, e.Ally, e.Mirror, e.WinLoss, e.GamesPlayed })
                    .HasName("index_WithHero");

                entity.Property(e => e.GameVersion)
                    .HasColumnName("game_version")
                    .HasColumnType("varchar(45)")
                    .HasCharSet("utf8")
                    .HasCollation("utf8_general_ci");

                entity.Property(e => e.GameType)
                    .HasColumnName("game_type")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.LeagueTier)
                    .HasColumnName("league_tier")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.HeroLeagueTier)
                    .HasColumnName("hero_league_tier")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.RoleLeagueTier)
                    .HasColumnName("role_league_tier")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.GameMap)
                    .HasColumnName("game_map")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.HeroLevel)
                    .HasColumnName("hero_level")
                    .HasColumnType("int(10) unsigned");

                entity.Property(e => e.Hero)
                    .HasColumnName("hero")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.Ally)
                    .HasColumnName("ally")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.Mirror)
                    .HasColumnName("mirror")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.WinLoss)
                    .HasColumnName("win_loss")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.GamesPlayed)
                    .HasColumnName("games_played")
                    .HasColumnType("int(11) unsigned");
            });

            modelBuilder.Entity<GlobalHeroMatchupsEnemy>(entity =>
            {
                entity.HasKey(e => new { e.GameVersion, e.GameType, e.LeagueTier, e.HeroLeagueTier, e.RoleLeagueTier, e.GameMap, e.HeroLevel, e.Hero, e.Enemy, e.Mirror, e.WinLoss })
                    .HasName("PRIMARY");

                entity.ToTable("global_hero_matchups_enemy");

                entity.HasIndex(e => new { e.GameVersion, e.GameType, e.Hero, e.LeagueTier, e.HeroLeagueTier, e.RoleLeagueTier, e.GameMap, e.HeroLevel, e.Enemy, e.Mirror, e.WinLoss, e.GamesPlayed })
                    .HasName("primary_withHero");

                entity.Property(e => e.GameVersion)
                    .HasColumnName("game_version")
                    .HasColumnType("varchar(45)")
                    .HasCharSet("utf8")
                    .HasCollation("utf8_general_ci");

                entity.Property(e => e.GameType)
                    .HasColumnName("game_type")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.LeagueTier)
                    .HasColumnName("league_tier")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.HeroLeagueTier)
                    .HasColumnName("hero_league_tier")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.RoleLeagueTier)
                    .HasColumnName("role_league_tier")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.GameMap)
                    .HasColumnName("game_map")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.HeroLevel)
                    .HasColumnName("hero_level")
                    .HasColumnType("int(10) unsigned");

                entity.Property(e => e.Hero)
                    .HasColumnName("hero")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.Enemy)
                    .HasColumnName("enemy")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.Mirror)
                    .HasColumnName("mirror")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.WinLoss)
                    .HasColumnName("win_loss")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.GamesPlayed)
                    .HasColumnName("games_played")
                    .HasColumnType("int(11) unsigned");
            });

            modelBuilder.Entity<GlobalHeroStats>(entity =>
            {
                entity.HasKey(e => new { e.GameVersion, e.GameType, e.LeagueTier, e.HeroLeagueTier, e.RoleLeagueTier, e.GameMap, e.HeroLevel, e.Hero, e.Mirror, e.WinLoss })
                    .HasName("PRIMARY");

                entity.ToTable("global_hero_stats");

                entity.HasIndex(e => new { e.GameVersion, e.GameType, e.LeagueTier, e.HeroLeagueTier, e.RoleLeagueTier, e.GameMap, e.HeroLevel, e.Hero, e.Mirror, e.WinLoss, e.GamesPlayed })
                    .HasName("primary_gamesPlayed");

                entity.Property(e => e.GameVersion)
                    .HasColumnName("game_version")
                    .HasColumnType("varchar(45)")
                    .HasCharSet("utf8")
                    .HasCollation("utf8_general_ci");

                entity.Property(e => e.GameType)
                    .HasColumnName("game_type")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.LeagueTier)
                    .HasColumnName("league_tier")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.HeroLeagueTier)
                    .HasColumnName("hero_league_tier")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.RoleLeagueTier)
                    .HasColumnName("role_league_tier")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.GameMap)
                    .HasColumnName("game_map")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.HeroLevel)
                    .HasColumnName("hero_level")
                    .HasColumnType("int(10) unsigned");

                entity.Property(e => e.Hero)
                    .HasColumnName("hero")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.Mirror)
                    .HasColumnName("mirror")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.WinLoss)
                    .HasColumnName("win_loss")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.Assists)
                    .HasColumnName("assists")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.ClutchHeals)
                    .HasColumnName("clutch_heals")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.CreepDamage)
                    .HasColumnName("creep_damage")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.DamageTaken)
                    .HasColumnName("damage_taken")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Deaths)
                    .HasColumnName("deaths")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Escapes)
                    .HasColumnName("escapes")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.ExperienceContribution)
                    .HasColumnName("experience_contribution")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.GameTime)
                    .HasColumnName("game_time")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.GamesPlayed)
                    .HasColumnName("games_played")
                    .HasColumnType("int(11) unsigned");

                entity.Property(e => e.Healing)
                    .HasColumnName("healing")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.HeroDamage)
                    .HasColumnName("hero_damage")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.HighestKillStreak)
                    .HasColumnName("highest_kill_streak")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Kills)
                    .HasColumnName("kills")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.MercCampCaptures)
                    .HasColumnName("merc_camp_captures")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.MinionDamage)
                    .HasColumnName("minion_damage")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Multikill)
                    .HasColumnName("multikill")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.OutnumberedDeaths)
                    .HasColumnName("outnumbered_deaths")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.PhysicalDamage)
                    .HasColumnName("physical_damage")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.ProtectionAllies)
                    .HasColumnName("protection_allies")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.RegenGlobes)
                    .HasColumnName("regen_globes")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.RootingEnemies)
                    .HasColumnName("rooting_enemies")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.SelfHealing)
                    .HasColumnName("self_healing")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.SiegeDamage)
                    .HasColumnName("siege_damage")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.SilencingEnemies)
                    .HasColumnName("silencing_enemies")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.SpellDamage)
                    .HasColumnName("spell_damage")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.StructureDamage)
                    .HasColumnName("structure_damage")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.StunningEnemies)
                    .HasColumnName("stunning_enemies")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.SummonDamage)
                    .HasColumnName("summon_damage")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Takedowns)
                    .HasColumnName("takedowns")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TeamfightDamageTaken)
                    .HasColumnName("teamfight_damage_taken")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TeamfightEscapes)
                    .HasColumnName("teamfight_escapes")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TeamfightHealing)
                    .HasColumnName("teamfight_healing")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TeamfightHeroDamage)
                    .HasColumnName("teamfight_hero_damage")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TimeCcEnemyHeroes)
                    .HasColumnName("time_cc_enemy_heroes")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TimeSpentDead)
                    .HasColumnName("time_spent_dead")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TownKills)
                    .HasColumnName("town_kills")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Vengeance)
                    .HasColumnName("vengeance")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.WatchTowerCaptures)
                    .HasColumnName("watch_tower_captures")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");
            });

            modelBuilder.Entity<GlobalHeroStatsBans>(entity =>
            {
                entity.HasKey(e => new { e.GameVersion, e.GameType, e.LeagueTier, e.HeroLeagueTier, e.RoleLeagueTier, e.GameMap, e.HeroLevel, e.Hero })
                    .HasName("PRIMARY");

                entity.ToTable("global_hero_stats_bans");

                entity.HasIndex(e => new { e.GameVersion, e.GameType, e.LeagueTier, e.HeroLeagueTier, e.RoleLeagueTier, e.GameMap, e.HeroLevel, e.Hero, e.Bans })
                    .HasName("Index_Bans");

                entity.Property(e => e.GameVersion)
                    .HasColumnName("game_version")
                    .HasColumnType("varchar(45)")
                    .HasCharSet("utf8")
                    .HasCollation("utf8_general_ci");

                entity.Property(e => e.GameType)
                    .HasColumnName("game_type")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.LeagueTier)
                    .HasColumnName("league_tier")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.HeroLeagueTier)
                    .HasColumnName("hero_league_tier")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.RoleLeagueTier)
                    .HasColumnName("role_league_tier")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.GameMap)
                    .HasColumnName("game_map")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.HeroLevel)
                    .HasColumnName("hero_level")
                    .HasColumnType("int(10)");

                entity.Property(e => e.Hero)
                    .HasColumnName("hero")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.Bans)
                    .HasColumnName("bans")
                    .HasColumnType("int(10) unsigned")
                    .HasDefaultValueSql("'0'");
            });

            modelBuilder.Entity<GlobalHeroStatsOld>(entity =>
            {
                entity.HasKey(e => new { e.GameVersion, e.GameType, e.Hero, e.WinLoss })
                    .HasName("PRIMARY");

                entity.ToTable("global_hero_stats_old");

                entity.Property(e => e.GameVersion)
                    .HasColumnName("game_version")
                    .HasColumnType("varchar(45)")
                    .HasCharSet("utf8")
                    .HasCollation("utf8_general_ci");

                entity.Property(e => e.GameType)
                    .HasColumnName("game_type")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.Hero)
                    .HasColumnName("hero")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.WinLoss)
                    .HasColumnName("win_loss")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.GamesPlayed)
                    .HasColumnName("games_played")
                    .HasColumnType("int(11) unsigned");
            });

            modelBuilder.Entity<GlobalHeroTalents>(entity =>
            {
                entity.HasKey(e => new { e.GameVersion, e.GameType, e.LeagueTier, e.HeroLeagueTier, e.RoleLeagueTier, e.GameMap, e.HeroLevel, e.Hero, e.Mirror, e.WinLoss, e.TalentCombinationId })
                    .HasName("PRIMARY");

                entity.ToTable("global_hero_talents");

                entity.HasIndex(e => new { e.GameVersion, e.GameType, e.Hero, e.LeagueTier, e.HeroLeagueTier, e.RoleLeagueTier, e.GameMap, e.HeroLevel, e.Mirror, e.WinLoss, e.TalentCombinationId, e.GamesPlayed })
                    .HasName("Base_Index");

                entity.Property(e => e.GameVersion)
                    .HasColumnName("game_version")
                    .HasColumnType("varchar(45)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.GameType)
                    .HasColumnName("game_type")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.LeagueTier)
                    .HasColumnName("league_tier")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.HeroLeagueTier)
                    .HasColumnName("hero_league_tier")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.RoleLeagueTier)
                    .HasColumnName("role_league_tier")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.GameMap)
                    .HasColumnName("game_map")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.HeroLevel)
                    .HasColumnName("hero_level")
                    .HasColumnType("int(10) unsigned");

                entity.Property(e => e.Hero)
                    .HasColumnName("hero")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.Mirror)
                    .HasColumnName("mirror")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.WinLoss)
                    .HasColumnName("win_loss")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.TalentCombinationId)
                    .HasColumnName("talent_combination_id")
                    .HasColumnType("int(11)");

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

                entity.Property(e => e.GameTime)
                    .HasColumnName("game_time")
                    .HasColumnType("int(11)");

                entity.Property(e => e.GamesPlayed)
                    .HasColumnName("games_played")
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

                entity.Property(e => e.MercCampCaptures)
                    .HasColumnName("merc_camp_captures")
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

            modelBuilder.Entity<GlobalHeroTalentsDetails>(entity =>
            {
                entity.HasKey(e => new { e.GameVersion, e.GameType, e.LeagueTier, e.HeroLeagueTier, e.RoleLeagueTier, e.GameMap, e.HeroLevel, e.Hero, e.Mirror, e.WinLoss, e.Level, e.Talent })
                    .HasName("PRIMARY");

                entity.ToTable("global_hero_talents_details");

                entity.HasIndex(e => new { e.GameVersion, e.GameType, e.Hero, e.LeagueTier, e.HeroLeagueTier, e.RoleLeagueTier, e.GameMap, e.HeroLevel, e.Mirror, e.WinLoss, e.Level, e.Talent, e.GamesPlayed })
                    .HasName("primary_withHero");

                entity.Property(e => e.GameVersion)
                    .HasColumnName("game_version")
                    .HasColumnType("varchar(45)")
                    .HasCharSet("utf8")
                    .HasCollation("utf8_general_ci");

                entity.Property(e => e.GameType)
                    .HasColumnName("game_type")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.LeagueTier)
                    .HasColumnName("league_tier")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.HeroLeagueTier)
                    .HasColumnName("hero_league_tier")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.RoleLeagueTier)
                    .HasColumnName("role_league_tier")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.GameMap)
                    .HasColumnName("game_map")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.HeroLevel)
                    .HasColumnName("hero_level")
                    .HasColumnType("int(10) unsigned");

                entity.Property(e => e.Hero)
                    .HasColumnName("hero")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.Mirror)
                    .HasColumnName("mirror")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.WinLoss)
                    .HasColumnName("win_loss")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.Level)
                    .HasColumnName("level")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Talent)
                    .HasColumnName("talent")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Assists)
                    .HasColumnName("assists")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.ClutchHeals)
                    .HasColumnName("clutch_heals")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.CreepDamage)
                    .HasColumnName("creep_damage")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.DamageTaken)
                    .HasColumnName("damage_taken")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Deaths)
                    .HasColumnName("deaths")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Escapes)
                    .HasColumnName("escapes")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.ExperienceContribution)
                    .HasColumnName("experience_contribution")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.GameTime)
                    .HasColumnName("game_time")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.GamesPlayed)
                    .HasColumnName("games_played")
                    .HasColumnType("int(10) unsigned")
                    .HasDefaultValueSql("'0'");

                entity.Property(e => e.Healing)
                    .HasColumnName("healing")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.HeroDamage)
                    .HasColumnName("hero_damage")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.HighestKillStreak)
                    .HasColumnName("highest_kill_streak")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Kills)
                    .HasColumnName("kills")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.MercCampCaptures)
                    .HasColumnName("merc_camp_captures")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.MinionDamage)
                    .HasColumnName("minion_damage")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Multikill)
                    .HasColumnName("multikill")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.OutnumberedDeaths)
                    .HasColumnName("outnumbered_deaths")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.PhysicalDamage)
                    .HasColumnName("physical_damage")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.ProtectionAllies)
                    .HasColumnName("protection_allies")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.RegenGlobes)
                    .HasColumnName("regen_globes")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.RootingEnemies)
                    .HasColumnName("rooting_enemies")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.SelfHealing)
                    .HasColumnName("self_healing")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.SiegeDamage)
                    .HasColumnName("siege_damage")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.SilencingEnemies)
                    .HasColumnName("silencing_enemies")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.SpellDamage)
                    .HasColumnName("spell_damage")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.StructureDamage)
                    .HasColumnName("structure_damage")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.StunningEnemies)
                    .HasColumnName("stunning_enemies")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.SummonDamage)
                    .HasColumnName("summon_damage")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Takedowns)
                    .HasColumnName("takedowns")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TeamfightDamageTaken)
                    .HasColumnName("teamfight_damage_taken")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TeamfightEscapes)
                    .HasColumnName("teamfight_escapes")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TeamfightHealing)
                    .HasColumnName("teamfight_healing")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TeamfightHeroDamage)
                    .HasColumnName("teamfight_hero_damage")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TimeCcEnemyHeroes)
                    .HasColumnName("time_cc_enemy_heroes")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TimeSpentDead)
                    .HasColumnName("time_spent_dead")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TownKills)
                    .HasColumnName("town_kills")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Vengeance)
                    .HasColumnName("vengeance")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.WatchTowerCaptures)
                    .HasColumnName("watch_tower_captures")
                    .HasColumnType("int(11) unsigned")
                    .HasDefaultValueSql("'NULL'");
            });

            modelBuilder.Entity<Heroes>(entity =>
            {
                entity.ToTable("heroes");

                entity.HasIndex(e => e.AttributeId)
                    .HasName("heroes_shortcut_index");

                entity.HasIndex(e => e.Name)
                    .HasName("heroes_name_index");

                entity.Property(e => e.Id)
                    .HasColumnName("id")
                    .HasColumnType("int(10) unsigned");

                entity.Property(e => e.AltName)
                    .HasColumnName("alt_name")
                    .HasColumnType("varchar(45)")
                    .HasDefaultValueSql("'NULL'")
                    .HasCharSet("utf8")
                    .HasCollation("utf8_general_ci");

                entity.Property(e => e.AttributeId)
                    .HasColumnName("attribute_id")
                    .HasColumnType("char(4)")
                    .HasDefaultValueSql("'NULL'")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.Name)
                    .IsRequired()
                    .HasColumnName("name")
                    .HasColumnType("varchar(255)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.NewRole)
                    .HasColumnName("new_role")
                    .HasColumnType("varchar(45)")
                    .HasDefaultValueSql("'NULL'")
                    .HasCharSet("utf8")
                    .HasCollation("utf8_general_ci");

                entity.Property(e => e.ReleaseDate)
                    .HasColumnName("release_date")
                    .HasColumnType("datetime")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.ReworkDate)
                    .HasColumnName("rework_date")
                    .HasColumnType("datetime")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Role)
                    .HasColumnName("role")
                    .HasColumnType("varchar(32)")
                    .HasDefaultValueSql("'NULL'")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.ShortName)
                    .IsRequired()
                    .HasColumnName("short_name")
                    .HasColumnType("varchar(32)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.Type)
                    .HasColumnName("type")
                    .HasColumnType("varchar(32)")
                    .HasDefaultValueSql("'NULL'")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");
            });

            modelBuilder.Entity<HeroesDataAbilities>(entity =>
            {
                entity.HasNoKey();

                entity.ToTable("heroes_data_abilities");

                entity.HasIndex(e => e.AttributeId)
                    .HasName("attribute_id");

                entity.HasIndex(e => new { e.HeroName, e.Title })
                    .HasName("name")
                    .IsUnique();

                entity.Property(e => e.AttributeId)
                    .IsRequired()
                    .HasColumnName("attribute_id")
                    .HasColumnType("varchar(10)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.Cooldown)
                    .IsRequired()
                    .HasColumnName("cooldown")
                    .HasColumnType("varchar(10)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.Description)
                    .IsRequired()
                    .HasColumnName("description")
                    .HasColumnType("varchar(500)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.HeroName)
                    .IsRequired()
                    .HasColumnName("hero_name")
                    .HasColumnType("varchar(100)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.Hotkey)
                    .IsRequired()
                    .HasColumnName("hotkey")
                    .HasColumnType("varchar(5)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.Icon)
                    .IsRequired()
                    .HasColumnName("icon")
                    .HasColumnType("varchar(500)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.ManaCost)
                    .IsRequired()
                    .HasColumnName("mana_cost")
                    .HasColumnType("varchar(10)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.Name)
                    .IsRequired()
                    .HasColumnName("name")
                    .HasColumnType("varchar(5)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.ShortName)
                    .IsRequired()
                    .HasColumnName("short_name")
                    .HasColumnType("varchar(100)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.Title)
                    .IsRequired()
                    .HasColumnName("title")
                    .HasColumnType("varchar(50)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.Trait)
                    .IsRequired()
                    .HasColumnName("trait")
                    .HasColumnType("varchar(5)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");
            });

            modelBuilder.Entity<HeroesDataTalents>(entity =>
            {
                entity.HasKey(e => new { e.TalentId, e.HeroName, e.Title, e.TalentName, e.Level })
                    .HasName("PRIMARY");

                entity.ToTable("heroes_data_talents");

                entity.HasIndex(e => e.AttributeId)
                    .HasName("attribute_id");

                entity.HasIndex(e => e.HeroName)
                    .HasName("name");

                entity.HasIndex(e => new { e.HeroName, e.Title, e.TalentName })
                    .HasName("heroTitleTalent");

                entity.HasIndex(e => new { e.HeroName, e.Title, e.TalentName, e.Level })
                    .HasName("Unique")
                    .IsUnique();

                entity.Property(e => e.TalentId)
                    .HasColumnName("talent_id")
                    .HasColumnType("int(10) unsigned")
                    .ValueGeneratedOnAdd();

                entity.Property(e => e.HeroName)
                    .HasColumnName("hero_name")
                    .HasColumnType("varchar(50)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.Title)
                    .HasColumnName("title")
                    .HasColumnType("varchar(50)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.TalentName)
                    .HasColumnName("talent_name")
                    .HasColumnType("varchar(100)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.Level)
                    .HasColumnName("level")
                    .HasColumnType("int(11)");

                entity.Property(e => e.AttributeId)
                    .IsRequired()
                    .HasColumnName("attribute_id")
                    .HasColumnType("varchar(10)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.Cooldown)
                    .IsRequired()
                    .HasColumnName("cooldown")
                    .HasColumnType("varchar(10)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.Description)
                    .IsRequired()
                    .HasColumnName("description")
                    .HasColumnType("varchar(500)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.Hotkey)
                    .IsRequired()
                    .HasColumnName("hotkey")
                    .HasColumnType("varchar(100)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.Icon)
                    .IsRequired()
                    .HasColumnName("icon")
                    .HasColumnType("varchar(100)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.ManaCost)
                    .IsRequired()
                    .HasColumnName("mana_cost")
                    .HasColumnType("varchar(10)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.RequiredTalentId)
                    .HasColumnName("required_talent_id")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.ShortName)
                    .IsRequired()
                    .HasColumnName("short_name")
                    .HasColumnType("varchar(50)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.Sort)
                    .IsRequired()
                    .HasColumnName("sort")
                    .HasColumnType("varchar(10)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.Status)
                    .HasColumnName("status")
                    .HasColumnType("varchar(45)")
                    .HasDefaultValueSql("'''playable'''")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");
            });

            modelBuilder.Entity<HeroesTranslations>(entity =>
            {
                entity.HasKey(e => new { e.Name, e.ShortName, e.Translation, e.AttributeId })
                    .HasName("PRIMARY");

                entity.ToTable("heroes_translations");

                entity.Property(e => e.Name)
                    .HasColumnName("name")
                    .HasColumnType("varchar(45)")
                    .HasCharSet("utf8")
                    .HasCollation("utf8_bin");

                entity.Property(e => e.ShortName)
                    .HasColumnName("short_name")
                    .HasColumnType("varchar(45)")
                    .HasCharSet("utf8")
                    .HasCollation("utf8_bin");

                entity.Property(e => e.Translation)
                    .HasColumnName("translation")
                    .HasColumnType("varchar(45)")
                    .HasCharSet("utf8")
                    .HasCollation("utf8_bin");

                entity.Property(e => e.AttributeId)
                    .HasColumnName("attribute_id")
                    .HasColumnType("varchar(45)")
                    .HasCharSet("utf8")
                    .HasCollation("utf8_bin");
            });

            modelBuilder.Entity<LeagueBreakdowns>(entity =>
            {
                entity.HasKey(e => new { e.TypeRoleHero, e.GameType, e.LeagueTier })
                    .HasName("PRIMARY");

                entity.ToTable("league_breakdowns");

                entity.HasIndex(e => e.MinMmr)
                    .HasName("min_mmr");

                entity.Property(e => e.TypeRoleHero)
                    .HasColumnName("type_role_hero")
                    .HasColumnType("int(11)");

                entity.Property(e => e.GameType)
                    .HasColumnName("game_type")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.LeagueTier)
                    .HasColumnName("league_tier")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.MinMmr)
                    .HasColumnName("min_mmr")
                    .HasDefaultValueSql("'NULL'");
            });

            modelBuilder.Entity<LeagueTiers>(entity =>
            {
                entity.HasKey(e => e.TierId)
                    .HasName("PRIMARY");

                entity.ToTable("league_tiers");

                entity.Property(e => e.TierId)
                    .HasColumnName("tier_id")
                    .HasColumnType("int(10) unsigned");

                entity.Property(e => e.Name)
                    .IsRequired()
                    .HasColumnName("name")
                    .HasColumnType("varchar(45)")
                    .HasCharSet("latin1")
                    .HasCollation("latin1_swedish_ci");
            });

            modelBuilder.Entity<Maps>(entity =>
            {
                entity.HasKey(e => new { e.MapId, e.Name })
                    .HasName("PRIMARY");

                entity.ToTable("maps");

                entity.Property(e => e.MapId)
                    .HasColumnName("map_id")
                    .HasColumnType("int(10) unsigned")
                    .ValueGeneratedOnAdd();

                entity.Property(e => e.Name)
                    .HasColumnName("name")
                    .HasColumnType("varchar(255)")
                    .HasCharSet("utf8")
                    .HasCollation("utf8_general_ci");

                entity.Property(e => e.ShortName)
                    .HasColumnName("short_name")
                    .HasColumnType("varchar(255)")
                    .HasDefaultValueSql("'NULL'")
                    .HasCharSet("utf8")
                    .HasCollation("utf8_general_ci");

                entity.Property(e => e.Type)
                    .HasColumnName("type")
                    .HasColumnType("varchar(45)")
                    .HasDefaultValueSql("'NULL'")
                    .HasCharSet("utf8")
                    .HasCollation("utf8_general_ci");
            });

            modelBuilder.Entity<MasterGamesPlayedData>(entity =>
            {
                entity.HasKey(e => new { e.TypeValue, e.Season, e.GameType, e.BlizzId, e.Region })
                    .HasName("PRIMARY");

                entity.ToTable("master_games_played_data");

                entity.HasIndex(e => e.GamesPlayed)
                    .HasName("games_played");

                entity.HasIndex(e => new { e.Season, e.GameType, e.GamesPlayed })
                    .HasName("1");

                entity.HasIndex(e => new { e.TypeValue, e.Season, e.GameType, e.Region })
                    .HasName("primaryMinusBlizzID");

                entity.Property(e => e.TypeValue)
                    .HasColumnName("type_value")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Season).HasColumnName("season");

                entity.Property(e => e.GameType)
                    .HasColumnName("game_type")
                    .HasColumnType("tinyint(4) unsigned");

                entity.Property(e => e.BlizzId)
                    .HasColumnName("blizz_id")
                    .HasColumnType("int(11) unsigned");

                entity.Property(e => e.Region)
                    .HasColumnName("region")
                    .HasColumnType("tinyint(3) unsigned");

                entity.Property(e => e.GamesPlayed)
                    .HasColumnName("games_played")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Loss)
                    .HasColumnName("loss")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Win)
                    .HasColumnName("win")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");
            });

            modelBuilder.Entity<MasterMmrData>(entity =>
            {
                entity.HasKey(e => new { e.TypeValue, e.GameType, e.BlizzId, e.Region })
                    .HasName("PRIMARY");

                entity.ToTable("master_mmr_data");

                entity.Property(e => e.TypeValue)
                    .HasColumnName("type_value")
                    .HasColumnType("int(11)");

                entity.Property(e => e.GameType)
                    .HasColumnName("game_type")
                    .HasColumnType("tinyint(4) unsigned");

                entity.Property(e => e.BlizzId)
                    .HasColumnName("blizz_id")
                    .HasColumnType("int(11) unsigned");

                entity.Property(e => e.Region)
                    .HasColumnName("region")
                    .HasColumnType("tinyint(3) unsigned");

                entity.Property(e => e.ConservativeRating).HasColumnName("conservative_rating");

                entity.Property(e => e.Loss)
                    .HasColumnName("loss")
                    .HasColumnType("int(11) unsigned");

                entity.Property(e => e.Mean).HasColumnName("mean");

                entity.Property(e => e.StandardDeviation).HasColumnName("standard_deviation");

                entity.Property(e => e.Win)
                    .HasColumnName("win")
                    .HasColumnType("int(11) unsigned");
            });

            modelBuilder.Entity<Migrations>(entity =>
            {
                entity.ToTable("migrations");

                entity.Property(e => e.Id)
                    .HasColumnName("id")
                    .HasColumnType("int(10) unsigned");

                entity.Property(e => e.Batch)
                    .HasColumnName("batch")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Migration)
                    .IsRequired()
                    .HasColumnName("migration")
                    .HasColumnType("varchar(255)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");
            });

            modelBuilder.Entity<MmrTypeIds>(entity =>
            {
                entity.HasKey(e => e.MmrTypeId)
                    .HasName("PRIMARY");

                entity.ToTable("mmr_type_ids");

                entity.Property(e => e.MmrTypeId)
                    .HasColumnName("mmr_type_id")
                    .HasColumnType("int(10) unsigned");

                entity.Property(e => e.Name)
                    .IsRequired()
                    .HasColumnName("name")
                    .HasColumnType("varchar(45)")
                    .HasCharSet("latin1")
                    .HasCollation("latin1_swedish_ci");
            });

            modelBuilder.Entity<Player>(entity =>
            {
                entity.HasKey(e => new { e.ReplayId, e.Battletag, e.Hero })
                    .HasName("PRIMARY");

                entity.ToTable("player");

                entity.HasIndex(e => new { e.BlizzId, e.Hero })
                    .HasName("blizzID_hero");

                entity.HasIndex(e => new { e.Hero, e.PlayerConservativeRating })
                    .HasName("hero_conservativeratinbg");

                entity.HasIndex(e => new { e.ReplayId, e.BlizzId, e.Hero })
                    .HasName("replayID_blizzID_Hero");

                entity.HasIndex(e => new { e.ReplayId, e.BlizzId, e.MmrDateParsed })
                    .HasName("replayID_blizzID_mmrDate");

                entity.HasIndex(e => new { e.ReplayId, e.Hero, e.PlayerConservativeRating })
                    .HasName("replayID_hero_ConservativeRating");

                entity.Property(e => e.ReplayId)
                    .HasColumnName("replayID")
                    .HasColumnType("int(10) unsigned");

                entity.Property(e => e.Battletag)
                    .HasColumnName("battletag")
                    .HasColumnType("varchar(50)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.Hero)
                    .HasColumnName("hero")
                    .HasColumnType("tinyint(3) unsigned");

                entity.Property(e => e.BlizzId)
                    .HasColumnName("blizz_id")
                    .HasColumnType("int(11) unsigned");

                entity.Property(e => e.HeroConservativeRating)
                    .HasColumnName("hero_conservative_rating")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.HeroLevel)
                    .HasColumnName("hero_level")
                    .HasColumnType("smallint(6) unsigned");

                entity.Property(e => e.HeroMean)
                    .HasColumnName("hero_mean")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.HeroStandardDeviation)
                    .HasColumnName("hero_standard_deviation")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.MasteryTaunt)
                    .HasColumnName("mastery_taunt")
                    .HasColumnType("smallint(6) unsigned")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.MmrDateParsed)
                    .HasColumnName("mmr_date_parsed")
                    .HasColumnType("datetime")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Party)
                    .HasColumnName("party")
                    .HasColumnType("varchar(45)")
                    .HasDefaultValueSql("'NULL'")
                    .HasCharSet("utf8")
                    .HasCollation("utf8_general_ci");

                entity.Property(e => e.PlayerConservativeRating)
                    .HasColumnName("player_conservative_rating")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.PlayerMean)
                    .HasColumnName("player_mean")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.PlayerStandardDeviation)
                    .HasColumnName("player_standard_deviation")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.RoleConservativeRating)
                    .HasColumnName("role_conservative_rating")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.RoleMean)
                    .HasColumnName("role_mean")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.RoleStandardDeviation)
                    .HasColumnName("role_standard_deviation")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Team)
                    .HasColumnName("team")
                    .HasColumnType("tinyint(4) unsigned");

                entity.Property(e => e.Winner)
                    .HasColumnName("winner")
                    .HasColumnType("tinyint(3) unsigned");
            });

            modelBuilder.Entity<Replay>(entity =>
            {
                entity.ToTable("replay");

                entity.HasIndex(e => e.GameDate)
                    .HasName("game_date");

                entity.HasIndex(e => e.GameVersion)
                    .HasName("gameversion");

                entity.HasIndex(e => e.ParsedId)
                    .HasName("parsed_id");

                entity.HasIndex(e => e.Region)
                    .HasName("region");

                entity.HasIndex(e => new { e.MmrRan, e.GameDate })
                    .HasName("mmr_ran_gameDate");

                entity.HasIndex(e => new { e.Region, e.GameType })
                    .HasName("region_gameType");

                entity.HasIndex(e => new { e.ReplayId, e.GameVersion })
                    .HasName("replayID_gameVersion");

                entity.HasIndex(e => new { e.ReplayId, e.GameType, e.GameDate })
                    .HasName("replayID_gameType_gameDate");

                entity.HasIndex(e => new { e.ReplayId, e.Region, e.GameDate })
                    .HasName("replayID_Region_GameDate");

                entity.HasIndex(e => new { e.ReplayId, e.Region, e.GameType, e.GameDate })
                    .HasName("replayID_Region_GameType");

                entity.HasIndex(e => new { e.ReplayId, e.Region, e.GameType, e.GameMap, e.GameDate })
                    .HasName("1");

                entity.Property(e => e.ReplayId)
                    .HasColumnName("replayID")
                    .HasColumnType("int(11) unsigned");

                entity.Property(e => e.DateAdded)
                    .HasColumnName("date_added")
                    .HasColumnType("datetime");

                entity.Property(e => e.GameDate)
                    .HasColumnName("game_date")
                    .HasColumnType("datetime");

                entity.Property(e => e.GameLength)
                    .HasColumnName("game_length")
                    .HasColumnType("smallint(5) unsigned");

                entity.Property(e => e.GameMap)
                    .HasColumnName("game_map")
                    .HasColumnType("tinyint(3) unsigned");

                entity.Property(e => e.GameType)
                    .HasColumnName("game_type")
                    .HasColumnType("tinyint(3) unsigned");

                entity.Property(e => e.GameVersion)
                    .IsRequired()
                    .HasColumnName("game_version")
                    .HasColumnType("varchar(32)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.MmrRan)
                    .HasColumnName("mmr_ran")
                    .HasColumnType("tinyint(4)")
                    .HasDefaultValueSql("'0'");

                entity.Property(e => e.ParsedId)
                    .HasColumnName("parsed_id")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Region)
                    .HasColumnName("region")
                    .HasColumnType("tinyint(3) unsigned");
            });

            modelBuilder.Entity<ReplayBans>(entity =>
            {
                entity.HasKey(e => e.BanId)
                    .HasName("PRIMARY");

                entity.ToTable("replay_bans");

                entity.HasIndex(e => new { e.ReplayId, e.Team, e.Hero })
                    .HasName("index");

                entity.Property(e => e.BanId)
                    .HasColumnName("ban_id")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Hero)
                    .HasColumnName("hero")
                    .HasColumnType("int(10) unsigned");

                entity.Property(e => e.ReplayId)
                    .HasColumnName("replayID")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Team)
                    .HasColumnName("team")
                    .HasColumnType("tinyint(3) unsigned");
            });

            modelBuilder.Entity<ReplayDraftOrder>(entity =>
            {
                entity.HasKey(e => new { e.ReplayId, e.Type, e.PickId })
                    .HasName("PRIMARY");

                entity.ToTable("replay_draft_order");

                entity.Property(e => e.ReplayId)
                    .HasColumnName("replayID")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Type)
                    .HasColumnName("type")
                    .HasColumnType("varchar(45)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_unicode_ci");

                entity.Property(e => e.PickId)
                    .HasColumnName("pick_id")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Hero)
                    .HasColumnName("hero")
                    .HasColumnType("int(11)");
            });

            modelBuilder.Entity<ReplayExperienceBreakdown>(entity =>
            {
                entity.HasKey(e => new { e.ReplayId, e.Team, e.TeamLevel, e.Timestamp })
                    .HasName("PRIMARY");

                entity.ToTable("replay_experience_breakdown");

                entity.Property(e => e.ReplayId)
                    .HasColumnName("replayID")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Team)
                    .HasColumnName("team")
                    .HasColumnType("tinyint(4)");

                entity.Property(e => e.TeamLevel)
                    .HasColumnName("team_level")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Timestamp)
                    .HasColumnName("timestamp")
                    .HasColumnType("varchar(50)")
                    .HasCharSet("utf8")
                    .HasCollation("utf8_general_ci");

                entity.Property(e => e.CreepXp)
                    .HasColumnName("creepXP")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.HeroXp)
                    .HasColumnName("heroXP")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.MinionXp)
                    .HasColumnName("minionXP")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.StructureXp)
                    .HasColumnName("structureXP")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TotalXp)
                    .HasColumnName("totalXP")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TrickXp)
                    .HasColumnName("trickXP")
                    .HasDefaultValueSql("'NULL'");
            });

            modelBuilder.Entity<Scores>(entity =>
            {
                entity.HasKey(e => new { e.ReplayId, e.Battletag })
                    .HasName("PRIMARY");

                entity.ToTable("scores");

                entity.Property(e => e.ReplayId)
                    .HasColumnName("replayID")
                    .HasColumnType("int(11) unsigned");

                entity.Property(e => e.Battletag)
                    .HasColumnName("battletag")
                    .HasColumnType("varchar(50)")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.Assists)
                    .HasColumnName("assists")
                    .HasColumnType("int(10)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.ClutchHeals)
                    .HasColumnName("clutch_heals")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.CreepDamage)
                    .HasColumnName("creep_damage")
                    .HasColumnType("int(10)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.DamageTaken)
                    .HasColumnName("damage_taken")
                    .HasColumnType("int(10)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Deaths)
                    .HasColumnName("deaths")
                    .HasColumnType("int(10)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Escapes)
                    .HasColumnName("escapes")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.ExperienceContribution)
                    .HasColumnName("experience_contribution")
                    .HasColumnType("int(10)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.FirstToTen)
                    .HasColumnName("first_to_ten")
                    .HasColumnType("tinyint(4)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Healing)
                    .HasColumnName("healing")
                    .HasColumnType("int(10)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.HeroDamage)
                    .HasColumnName("hero_damage")
                    .HasColumnType("int(10)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.HighestKillStreak)
                    .HasColumnName("highest_kill_streak")
                    .HasColumnType("int(10)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Kills)
                    .HasColumnName("kills")
                    .HasColumnType("int(10)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Level)
                    .HasColumnName("level")
                    .HasColumnType("int(10)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.MatchAward)
                    .HasColumnName("match_award")
                    .HasColumnType("varchar(10)")
                    .HasDefaultValueSql("'NULL'")
                    .HasCharSet("utf8mb4")
                    .HasCollation("utf8mb4_general_ci");

                entity.Property(e => e.MercCampCaptures)
                    .HasColumnName("merc_camp_captures")
                    .HasColumnType("int(10)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.MetaExperience)
                    .HasColumnName("meta_experience")
                    .HasColumnType("int(10)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.MinionDamage)
                    .HasColumnName("minion_damage")
                    .HasColumnType("int(10)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Multikill)
                    .HasColumnName("multikill")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.OutnumberedDeaths)
                    .HasColumnName("outnumbered_deaths")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.PhysicalDamage)
                    .HasColumnName("physical_damage")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.ProtectionAllies)
                    .HasColumnName("protection_allies")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.RegenGlobes)
                    .HasColumnName("regen_globes")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.RootingEnemies)
                    .HasColumnName("rooting_enemies")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.SelfHealing)
                    .HasColumnName("self_healing")
                    .HasColumnType("int(10)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.SiegeDamage)
                    .HasColumnName("siege_damage")
                    .HasColumnType("int(10)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.SilencingEnemies)
                    .HasColumnName("silencing_enemies")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.SpellDamage)
                    .HasColumnName("spell_damage")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.StructureDamage)
                    .HasColumnName("structure_damage")
                    .HasColumnType("int(10)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.StunningEnemies)
                    .HasColumnName("stunning_enemies")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.SummonDamage)
                    .HasColumnName("summon_damage")
                    .HasColumnType("int(10)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Takedowns)
                    .HasColumnName("takedowns")
                    .HasColumnType("int(10)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TeamfightDamageTaken)
                    .HasColumnName("teamfight_damage_taken")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TeamfightEscapes)
                    .HasColumnName("teamfight_escapes")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TeamfightHealing)
                    .HasColumnName("teamfight_healing")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TeamfightHeroDamage)
                    .HasColumnName("teamfight_hero_damage")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TimeCcEnemyHeroes)
                    .HasColumnName("time_cc_enemy_heroes")
                    .HasColumnType("int(10)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TimeSpentDead)
                    .HasColumnName("time_spent_dead")
                    .HasColumnType("int(10)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.TownKills)
                    .HasColumnName("town_kills")
                    .HasColumnType("int(10)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.Vengeance)
                    .HasColumnName("vengeance")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.WatchTowerCaptures)
                    .HasColumnName("watch_tower_captures")
                    .HasColumnType("int(10)")
                    .HasDefaultValueSql("'NULL'");
            });

            modelBuilder.Entity<SeasonDates>(entity =>
            {
                entity.ToTable("season_dates");

                entity.Property(e => e.Id)
                    .HasColumnName("id")
                    .HasColumnType("int(11) unsigned");

                entity.Property(e => e.EndDate)
                    .HasColumnName("end_date")
                    .HasColumnType("datetime");

                entity.Property(e => e.Season)
                    .HasColumnName("season")
                    .HasColumnType("double unsigned");

                entity.Property(e => e.StartDate)
                    .HasColumnName("start_date")
                    .HasColumnType("datetime");

                entity.Property(e => e.Year)
                    .HasColumnName("year")
                    .HasColumnType("int(10) unsigned");
            });

            modelBuilder.Entity<SeasonGameVersions>(entity =>
            {
                entity.HasKey(e => new { e.Season, e.GameVersion })
                    .HasName("PRIMARY");

                entity.ToTable("season_game_versions");

                entity.Property(e => e.Season)
                    .HasColumnName("season")
                    .HasColumnType("int(11)");

                entity.Property(e => e.GameVersion)
                    .HasColumnName("game_version")
                    .HasColumnType("varchar(45)")
                    .HasCharSet("utf8")
                    .HasCollation("utf8_general_ci");
            });

            modelBuilder.Entity<TalentCombinations>(entity =>
            {
                entity.HasKey(e => e.TalentCombinationId)
                    .HasName("PRIMARY");

                entity.ToTable("talent_combinations");

                entity.HasIndex(e => new { e.Hero, e.LevelTwenty })
                    .HasName("index");

                entity.HasIndex(e => new { e.Hero, e.LevelOne, e.LevelFour, e.LevelSeven, e.LevelTen, e.LevelThirteen, e.LevelSixteen, e.LevelTwenty })
                    .HasName("Unique")
                    .IsUnique();

                entity.Property(e => e.TalentCombinationId)
                    .HasColumnName("talent_combination_id")
                    .HasColumnType("int(11)");

                entity.Property(e => e.Hero)
                    .HasColumnName("hero")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.LevelFour)
                    .HasColumnName("level_four")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.LevelOne)
                    .HasColumnName("level_one")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.LevelSeven)
                    .HasColumnName("level_seven")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.LevelSixteen)
                    .HasColumnName("level_sixteen")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.LevelTen)
                    .HasColumnName("level_ten")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.LevelThirteen)
                    .HasColumnName("level_thirteen")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.LevelTwenty)
                    .HasColumnName("level_twenty")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");
            });

            modelBuilder.Entity<Talents>(entity =>
            {
                entity.HasKey(e => new { e.ReplayId, e.Battletag })
                    .HasName("PRIMARY");

                entity.ToTable("talents");

                entity.Property(e => e.ReplayId)
                    .HasColumnName("replayID")
                    .HasColumnType("int(11) unsigned");

                entity.Property(e => e.Battletag)
                    .HasColumnName("battletag")
                    .HasColumnType("int(11)");

                entity.Property(e => e.LevelFour)
                    .HasColumnName("level_four")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.LevelOne)
                    .HasColumnName("level_one")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.LevelSeven)
                    .HasColumnName("level_seven")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.LevelSixteen)
                    .HasColumnName("level_sixteen")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.LevelTen)
                    .HasColumnName("level_ten")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.LevelThirteen)
                    .HasColumnName("level_thirteen")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");

                entity.Property(e => e.LevelTwenty)
                    .HasColumnName("level_twenty")
                    .HasColumnType("int(11)")
                    .HasDefaultValueSql("'NULL'");
            });

            OnModelCreatingPartial(modelBuilder);
        }

        partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
    }
}
