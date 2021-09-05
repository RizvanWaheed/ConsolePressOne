using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace ConsolePressOne
{
    class Program
    {
        static void Main(string[] args)
        {
            ConsoleKeyInfo cki;
            Console.Clear();
            // Establish an event handler to process key press events.
            Console.CancelKeyPress += new ConsoleCancelEventHandler(consoleCustomeHandler);
            // Console.WriteLine("Hello World!");
            // Keep the console window open in debug mode.
            Console.WriteLine("Press any key to exit.");

            Timer timer = new Timer();
            timer.Elapsed += new ElapsedEventHandler(OnTimedEvent);
            timer.Interval = 5000; //number in milisecinds  
            timer.Enabled = true;

             cki = Console.ReadKey(true);
            var cki2 = Console.Read();
            var cki3 = Console.ReadLine();
        }
        protected static void consoleCustomeHandler(object sender, ConsoleCancelEventArgs args)
        {
            Console.WriteLine("\nThe read operation has been interrupted.");

            Console.WriteLine("  Key pressed: {0}", args.SpecialKey);

            Console.WriteLine("  Cancel property: {0}", args.Cancel);

            // Set the Cancel property to true to prevent the process from terminating.
            Console.WriteLine("Setting the Cancel property to true...");
            args.Cancel = true;

            // Announce the new value of the Cancel property.
            Console.WriteLine("  Cancel property: {0}", args.Cancel);
            Console.WriteLine("The read operation will resume...\n");
        }
        private static void OnTimedEvent(object source, ElapsedEventArgs e)
        {
            Console.WriteLine("I am in time lapsed.");
            //new Service();
            Services svc = new Services();
            svc.SendDialCallServices();
        }
    }
}
