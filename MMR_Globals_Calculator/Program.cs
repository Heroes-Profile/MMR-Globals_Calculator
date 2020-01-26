using System;
using System.IO;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration.Json;
using MMR_Globals_Calculator.Database.HeroesProfile;
using MMR_Globals_Calculator.Database.HeroesProfileBrawl;
using MMR_Globals_Calculator.Database.HeroesProfileCache;
using MMR_Globals_Calculator.Helpers;
using MMR_Globals_Calculator.Models;


namespace MMR_Globals_Calculator
{
    public class Program
    {
        //We need this in ConsoleApp.cs since we can't DI into a static class
        public static ServiceProvider ServiceProviderProvider;
        private static void Main(string[] args)
        {
            // Create service collection and configure our services
            var services = ConfigureServices();
            // Generate a provider
            var serviceProvider = services.BuildServiceProvider();
            ServiceProviderProvider = serviceProvider;
   
            // Kick off our actual code
            ConsoleApp.Run();
        }
        
        private static IServiceCollection ConfigureServices()
        {
            IServiceCollection services = new ServiceCollection();
            
            
            // Set up the objects we need to get to configuration settings
            var config = LoadConfiguration();
            var dbSettings = config.GetSection("DbSettings").Get<DbSettings>();
            // Add the config to our DI container for later use
            services.AddSingleton(config);
            services.AddSingleton(dbSettings);

            // EF Db config
            services.AddDbContext<HeroesProfileContext>(options => options.UseMySql(ConnectionStringBuilder.BuildConnectionString(dbSettings)));
            dbSettings.database = "heroesprofile_brawl";
            services.AddDbContext<HeroesProfileBrawlContext>(options => options.UseMySql(ConnectionStringBuilder.BuildConnectionString(dbSettings)));
            dbSettings.database = "heroesprofile_cache";
            services.AddDbContext<HeroesProfileCacheContext>(options => options.UseMySql(ConnectionStringBuilder.BuildConnectionString(dbSettings)));
            dbSettings.database = "heroesprofile";

            services.AddScoped<RunMmrService>();
            services.AddScoped<MmrCalculatorService>();

            // IMPORTANT! Register our application entry point
            services.AddTransient<ConsoleApp>();
            return services;
        }

        private static IConfiguration LoadConfiguration()
        {
            var builder = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json")
                    .AddEnvironmentVariables();
            return  builder.Build();
        }
    }
    

}