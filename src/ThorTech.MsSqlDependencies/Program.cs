using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;

namespace ThorTech.MsSqlDependencies
{
    public class StoredProcedureDependency
    {
        public int Depth { get; set; }
        
        public string Name { get; set; }
        
        public string Parent { get; set; }

        public override string ToString()
        {
            return $"{Depth},{Name},{Parent}";
        }
    }
    
    public class Program
    {
        public static void Main(string[] args)
        {
            var storedProcedure = args[0];
            var outputFile = args[1];
            var connectionString = args[2];


            var sprocHierarchy = GetHierarchy(connectionString, storedProcedure);

            var allLines = sprocHierarchy.Select(s => s.ToString());
            
            File.WriteAllLines(outputFile, allLines);
        }

        private static IList<StoredProcedureDependency> GetHierarchy(string connectionString, string storedProcedure)
        {
            var allSprocs = new Dictionary<string, StoredProcedureDependency>();
            var readQueue = new Queue<(string sproc, int depth)>();
            allSprocs.Add(storedProcedure, new StoredProcedureDependency
            {
                Depth = 0, 
                Name = storedProcedure,
                Parent = null
            });
            readQueue.Enqueue((storedProcedure, 0));

            using (var con = new SqlConnection(connectionString))
            {
                con.Open();

                while (readQueue.TryDequeue(out var item))
                {
                    var (sproc, depth) = item;
                    var currDepth = depth + 1;
                    var sprocs = ReadSprocs(con, sproc);
                    var dedupedSprocs = sprocs.Where(s => !allSprocs.Keys.Contains(s));
                    foreach (var dedupedSproc in dedupedSprocs)
                    {
                        var record = new StoredProcedureDependency
                        {
                            Depth = currDepth,
                            Name = dedupedSproc,
                            Parent = sproc
                        };
                        allSprocs.Add(dedupedSproc, record);
                        readQueue.Enqueue((dedupedSproc, currDepth));
                    }
                }
                
                con.Close();
            }

            return allSprocs.Values.ToList();
        }

        private static ISet<string> ReadSprocs(SqlConnection con, string storedProcedure)
        {
            Console.WriteLine($"Determining dependencies for sproc  Sproc={storedProcedure}");
            var results = new HashSet<string>();
            var sql = $"EXEC sp_depends '{storedProcedure}'";
            using (var cmd = new SqlCommand(sql, con))
            {
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var sprocName = reader.GetString(0);
                    var depType = reader.GetString(1);

                    if (reader.GetColumnSchema().Count != 5)
                    {
                        // This contains dependents instead of dependencies
                        continue;
                    }
                    
                    if (depType == "stored procedure" || depType == "scalar function")
                    {
                        results.Add(sprocName);
                    }
                }

                reader.Close();
            }

            return results;
        }
    }
}
