using Microsoft.Extensions.DependencyInjection;
using MMR_Globals_Calculator.Database.HeroesProfile;
using MMR_Globals_Calculator.Models;
using static MMR_Globals_Calculator.Program;

namespace MMR_Globals_Calculator
{
    public class ConsoleApp
    {
        public static void Run()
        {
            var runMmrService = ServiceProviderProvider.GetService<RunMmrService>();

            var c = runMmrService.RunMmr();
        }
    }
}