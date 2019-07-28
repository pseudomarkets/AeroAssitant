using System;
using System.Collections.Generic;
using Aerospike.Client;

namespace AeroAssistant
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                Console.WriteLine("AeroAssistant");
                Console.WriteLine("(c) 2019 The IoTAware Development Team");
                Console.Write("Enter host IP: ");
                string ip = Console.ReadLine();
                Console.Write("Enter host port: ");
                int port = Int32.Parse(Console.ReadLine());
                AerospikeClient client = new AerospikeClient(ip, port);
                if (client.Connected)
                {
                    Console.WriteLine("Connected to aerospike cluster on host: " + ip);
                    LaunchAssistant(client);
                }
                else
                {
                    Console.WriteLine("Could not connect to aerospike cluster on host: " + ip);
                    Console.ReadKey();
                }
            }
            catch(Exception e)
            {
                Console.WriteLine("Could not connect to host");
                Console.ReadKey();
            }

        }

        public static void LaunchAssistant(AerospikeClient client)
        {
            Console.WriteLine("****AeroAssistant Main Menu****");
            Console.WriteLine("1) Read records");
            Console.WriteLine("2) Write record");
            Console.WriteLine("3) Exit");
            Console.Write("Enter selection: ");
            string input = Console.ReadLine();
            switch(input)
            {
                case "1":
                    ReadRecords(client);
                    break;
                case "2":
                    WriteRecord(client);
                    break;
                case "3":
                    client.Close();
                    break;
               
            }
        }

        public static void ReadRecords(AerospikeClient client)
        {
            Console.Write("Enter namespace: ");
            string ns = Console.ReadLine();
            Console.Write("Enter set name: ");
            string setName = Console.ReadLine();
            Console.Write("Enter key name: ");
            string keyName = Console.ReadLine();
            Key key = new Key(ns, setName, keyName);
            Record record = client.Get(null, key);
            if(record != null)
            {
                foreach (KeyValuePair<string, object> entry in record.bins)
                {
                    Console.WriteLine("Name: " + entry.Key + " Value: " + entry.Value);
                }
            }
            LaunchAssistant(client);
        }

        public static void WriteRecord(AerospikeClient client)
        {
            WritePolicy policy = new WritePolicy();
            policy.SetTimeout(50);

            Console.Write("Enter namespace: ");
            string ns = Console.ReadLine();
            Console.Write("Enter set name: ");
            string setName = Console.ReadLine();
            Console.Write("Enter key name: ");
            string keyName = Console.ReadLine();
            Console.Write("Enter bin name: ");
            string binName = Console.ReadLine();
            Console.Write("Enter value: ");
            string value = Console.ReadLine();

            Key key = new Key(ns, setName, keyName);
            Bin bin = new Bin(binName, value);

            client.Put(policy, key, bin);
            Console.WriteLine("Record written to DB");
            LaunchAssistant(client);
        }

    }
}
