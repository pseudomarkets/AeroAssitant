using System;
using System.Collections.Generic;
using Aerospike.Client;

namespace AeroAssistant
{
    public class Program
    {
        private static AerospikeClient _client;
        static void Main(string[] args)
        {
            try
            {
                Console.WriteLine("AeroAssistant");
                Console.WriteLine("(c) 2019 - 2020 Shravan Jambukesan");
                Console.Write("Enter hostname or IP: ");
                string ip = Console.ReadLine();
                Console.Write("Enter host port: ");
                int port = Int32.Parse(Console.ReadLine());
                _client = new AerospikeClient(ip, port);
                if (_client.Connected)
                {
                    Console.WriteLine("Connected to Aerospike cluster on host: " + ip + ":" + port);
                    LaunchAssistant();
                }
                else
                {
                    Console.WriteLine("Could not connect to Aerospike cluster on host: " + ip + ":" + port);
                    Console.ReadKey();
                }
            }
            catch(Exception e)
            {
                Console.WriteLine("Could not connect to host");
                Console.WriteLine("The following error occured: " + e);
                Console.ReadKey();
            }

        }

        public static void LaunchAssistant()
        {
            Console.WriteLine("**** AeroAssistant Main Menu ****");
            Console.WriteLine("1) Read records");
            Console.WriteLine("2) Write record");
            Console.WriteLine("3) Delete record");
            Console.WriteLine("4) Exit");
            Console.Write("Enter selection: ");
            string input = Console.ReadLine();
            switch(input)
            {
                case "1":
                    ReadRecords();
                    break;
                case "2":
                    WriteRecord();
                    break;
                case "3":
                    DeleteRecord();
                    break;
                case "4":
                    _client.Close();
                    break;
               
            }
        }

        public static void ReadRecords()
        {
            try
            {
                Console.Write("Enter namespace: ");
                string ns = Console.ReadLine();
                Console.Write("Enter set name: ");
                string setName = Console.ReadLine();
                Console.Write("Enter key: ");
                string keyName = Console.ReadLine();
                Key key = new Key(ns, setName, keyName);
                Record record = _client.Get(null, key);
                Console.WriteLine("Query Result");
                Console.WriteLine("======================================");
                if(record != null)
                {
                    foreach (KeyValuePair<string, object> entry in record.bins)
                    {
                        Console.WriteLine("Name: " + entry.Key + " Value: " + entry.Value);
                    }
                }
                Console.WriteLine("======================================");
                LaunchAssistant();
            }
            catch (Exception e)
            {
                Console.WriteLine("The following error occured while trying to read a record: ");
                Console.WriteLine(e);
                LaunchAssistant();
            }

        }

        public static void DeleteRecord()
        {
            try
            {
                WritePolicy policy = new WritePolicy();
                policy.SetTimeout(50);
                Console.Write("Enter namespace: ");
                string ns = Console.ReadLine();
                Console.Write("Enter set name: ");
                string setName = Console.ReadLine();
                Console.Write("Enter key: ");
                string keyName = Console.ReadLine();
                Key key = new Key(ns, setName, keyName);
                _client.Delete(policy, key);
                Console.WriteLine("Record deleted");
                LaunchAssistant();
            }
            catch(Exception e)
            {
                Console.WriteLine("The following error occured while trying to delete a record: ");
                Console.WriteLine(e);
                LaunchAssistant();
            }
        }

        public static void WriteRecord()
        {
            try
            {
                Bin bin = null;
                WritePolicy policy = new WritePolicy();
                policy.SetTimeout(50);
                Console.Write("Enter namespace: ");
                string ns = Console.ReadLine();
                Console.Write("Enter set name: ");
                string setName = Console.ReadLine();
                Console.Write("Enter key: ");
                string keyName = Console.ReadLine();
                Console.Write("Enter bin name: ");
                string binName = Console.ReadLine();
                Console.Write("Enter in data type for value (string, int, double, bool): ");
                string dataType = Console.ReadLine();
                Console.Write("Enter value: ");
                string value = Console.ReadLine();

                switch (dataType)
                {
                    case "string":
                        bin = new Bin(binName, value);
                        break;
                    case "int":
                        bin = new Bin(binName, Int32.Parse(value));
                        break;
                    case "double":
                        bin = new Bin(binName, double.Parse(value));
                        break;
                    case "bool":
                        bin = new Bin(binName, bool.Parse(value));
                        break;
                    default:
                        bin = new Bin(binName, value);
                        break;
                }

                Key key = new Key(ns, setName, keyName);

                _client.Put(policy, key, bin);
                Console.WriteLine("Record written to DB");
                LaunchAssistant();
            }
            catch (Exception e)
            {
                Console.WriteLine("The following error occured while trying to write a record: ");
                Console.WriteLine(e);
                LaunchAssistant();
            }

        }

    }
}
