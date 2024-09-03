<Query Kind="Program">
  <NuGetReference>Connection</NuGetReference>
  <NuGetReference>CsvHelper</NuGetReference>
  <NuGetReference>CsvReader.dll</NuGetReference>
  <NuGetReference>CsvReaderAdvanced</NuGetReference>
  <NuGetReference>Dapper</NuGetReference>
  <NuGetReference>Microsoft.SqlServer.ConnectionInfo.dll</NuGetReference>
  <NuGetReference>System.Data.SqlClient</NuGetReference>
  <Namespace>CsvHelper</Namespace>
  <Namespace>CsvHelper.Configuration</Namespace>
  <Namespace>CsvHelper.Configuration.Attributes</Namespace>
  <Namespace>CsvHelper.Delegates</Namespace>
  <Namespace>CsvHelper.Expressions</Namespace>
  <Namespace>CsvHelper.TypeConversion</Namespace>
  <Namespace>Dapper</Namespace>
  <Namespace>System.Data.SqlClient</Namespace>
  <Namespace>System.Globalization</Namespace>
  <Namespace>System.IO.Compression</Namespace>
  <Namespace>System.Net.Http</Namespace>
  <Namespace>System.Threading.Tasks</Namespace>
</Query>


public class Pzena_Interview
{
	static async Task Main(string[] args)
	{
		// URL of the file to download
		string dbConnectionString = "Server=localhost;Database=PZENA;Integrated Security=True;";
		string urlTickers = "https://www.alphaforge.net/A0B1C3/TICKERS.zip";
		string urlPrices = "https://www.alphaforge.net/A0B1C3/PRICES.zip";
		string dirLocal = "C:\\Temp\\";
		string fileZipTickers = "TICKERS.zip";
		string fileZipPrices = "PRICES.zip";
		string fileNameTickers = "TICKERS.csv";
		string fileNamePrices = "PRICES.csv";



		//		// Call the method to download the file
		await DownloadFileAsync(urlTickers, dirLocal + fileZipTickers);
		await DownloadFileAsync(urlPrices, dirLocal + fileZipPrices);
		
		UnzipFile(dirLocal + fileZipTickers, dirLocal );
		UnzipFile(dirLocal + fileZipPrices, dirLocal);

		CreateStagingTableFromCSVFile(dirLocal + fileNameTickers, dbConnectionString, fileNameTickers);
		CreateStagingTableFromCSVFile(dirLocal + fileNamePrices, dbConnectionString, fileNamePrices);

		LoadFileToStaging(dirLocal + fileNameTickers, dbConnectionString, fileNameTickers);
		LoadFileToStaging(dirLocal + fileNamePrices, dbConnectionString, fileNamePrices);
	}



	static void CreateStagingTableFromCSVFile(string filePathName, string dbConnectionString, string fileName)
	{
		// reads CSV header structure into an array, creates a staging table with generic field type
		// to load the data into. data is loaded in a separate proc LoadFileToStaging
		
		string[] headers;
		string createTableQuery;
		string tableName = "STG_" + fileName.Substring(0, fileName.Length - 4); // strip .csv extention from filename

		try
		{
			using (var connection = new SqlConnection(dbConnectionString))
			{
				connection.Open();
				// Read the first line of the CSV file
				using (StreamReader reader = new StreamReader(filePathName))
				using (var myCsvReader = new CsvReader(reader, CultureInfo.InvariantCulture))
				{
					myCsvReader.Read();
					myCsvReader.ReadHeader();
					headers = myCsvReader.HeaderRecord;

					//// Define the SQL for creating the table dynamically based on the headers
					createTableQuery = $@"IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{tableName}') CREATE TABLE {tableName} ({string.Join(", ", headers.Select(h => $"[{h}] NVARCHAR(MAX)"))})";
					Console.WriteLine(createTableQuery);
					connection.Execute(createTableQuery);
					connection.Close();
				}
				Console.WriteLine("Table created or already exits successfully.");
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"An error occurred: {ex.Message}");
		}
	}

	static void LoadFileToStaging(string filePathName, string dbConnectionString, string fileName)
	{
		// uses bulk copy to load data from csv file into staging table.
		int batchSize = 10000; // Adjust based on your needs and memory constraints
		string tableName = "STG_" + fileName.Substring(0, fileName.Length - 4); // strip .csv extention from filename
		string[] headers;

		try
		{
			using (var connection = new SqlConnection(dbConnectionString))
			{
				connection.Open();
				// Read CSV header and create table
				using (var reader = new StreamReader(filePathName))
				using (var csvReader = new CsvReader(reader, CultureInfo.InvariantCulture))
				{
					csvReader.Read();
					csvReader.ReadHeader();
					headers = csvReader.HeaderRecord;

				// Prepare SqlBulkCopy
				using (var bulkCopy = new SqlBulkCopy(connection))
				{
					bulkCopy.DestinationTableName = tableName;
					bulkCopy.BatchSize = batchSize;
					bulkCopy.BulkCopyTimeout = 0; // No timeout

					// Set up column mappings
					foreach (var header in headers)
					{
						bulkCopy.ColumnMappings.Add(header, header);
					}

					// Read and bulk insert data
						using (var dr = new CsvDataReader(csvReader))
						{
							bulkCopy.WriteToServer(dr);
						}
					}
				}
				Console.WriteLine("Data insertion completed successfully.");
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"An error occurred: {ex.Message}");
		}
	}


	// Helper method to execute SQL commands
	void ExecuteCommand(string sql, string dbConnectionString)
	{
		using (SqlConnection connection = new SqlConnection(dbConnectionString))
		{
			try
			{
				// Open the connection
				connection.Open();
				Console.WriteLine("Connection successful!");
				using (var command = new SqlCommand(sql, connection))
				{
					command.ExecuteNonQuery();
				}
				connection.Close();
			}
			catch (Exception ex)
			{
				Console.WriteLine($"An error occurred: {ex.Message}");
			}
		}
	}

	// Downloading file from web site to a local directory
	static async Task DownloadFileAsync(string url, string outputPath)
	{
		const int BUFFER_SIZE = 16 * 1024;

		using (HttpClient client = new HttpClient())
		{
			try
			{
				// Send a GET request to the URL
                HttpResponseMessage response = await client.GetAsync(url, HttpCompletionOption.ResponseHeadersRead);
                
                // Ensure the request was successful
                response.EnsureSuccessStatusCode();
				
                // Open the response stream
                using (Stream contentStream = await response.Content.ReadAsStreamAsync(), 
                              fileStream = new FileStream(outputPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: BUFFER_SIZE, useAsync: true))
                {
                    // Read the content stream in chunks and write to the file
                    byte[] buffer = new byte[BUFFER_SIZE];
                    int bytesRead;
                    while ((bytesRead = await contentStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
                    {
                        await fileStream.WriteAsync(buffer, 0, bytesRead);
                    }
                }

                Console.WriteLine("File downloaded successfully.");
            }
            catch (HttpRequestException httpEx)
            {
                Console.WriteLine($"HTTP error occurred: {httpEx.Message}");
            }
            catch (UnauthorizedAccessException uaEx)
            {
                Console.WriteLine($"Access denied: {uaEx.Message}");
            }
            catch (IOException ioEx)
            {
                Console.WriteLine($"I/O error occurred: {ioEx.Message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An unexpected error occurred: {ex.Message}");
            }
        }
    }
		
	// unzipping the compressed file
	static void UnzipFile(string zipFilePath, string extractPath)
    {
        try
        {
            // Ensure the zip file exists
            if (!File.Exists(zipFilePath))
            {
                Console.WriteLine("Zip file not found.");
                return;
            }

            // Check if the extraction directory exists
            if (!Directory.Exists(extractPath))
            {
                Console.WriteLine("The extraction directory does not exist.");
                return;
            }

            // Check if there are write permissions on the extraction directory
            if (!HasWritePermission(extractPath))
            {
                Console.WriteLine("No write permissions for the extraction directory.");
                return;
            }

            // Get the drive information for the extraction path
            DriveInfo drive = new DriveInfo(Path.GetPathRoot(extractPath));

            // Estimate space needed (this is a simple check, assuming extracted size is similar to zip size)
            FileInfo zipInfo = new FileInfo(zipFilePath);
            long estimatedSize = zipInfo.Length;

            // Check if there is enough free space on the disk
            if (drive.AvailableFreeSpace < estimatedSize)
            {
                Console.WriteLine("Not enough disk space to extract the zip file.");
                return;
            }



            // Remove any existing files in the extraction directory with the same name as the files in the zip
            using (ZipArchive archive = ZipFile.OpenRead(zipFilePath))
            {
                foreach (ZipArchiveEntry entry in archive.Entries)
                {
                    string destinationPath = Path.Combine(extractPath, entry.FullName);
                    if (File.Exists(destinationPath))
                    {
                        File.Delete(destinationPath);
                        Console.WriteLine($"Removed existing file: {entry.FullName}");
                    }
                }
            }

            // Extract the zip file
            ZipFile.ExtractToDirectory(zipFilePath, extractPath);
            Console.WriteLine("Zip file extracted successfully.");
        }
        catch (UnauthorizedAccessException uaEx)
        {
            Console.WriteLine($"Access denied: {uaEx.Message}");
        }
        catch (IOException ioEx)
        {
            Console.WriteLine($"I/O error occurred: {ioEx.Message}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An unexpected error occurred: {ex.Message}");
        }
    }

	// helper method to check the write permissions on a directory
	static bool HasWritePermission(string directoryPath)
	{
		try
		{
			// Attempt to create a temporary file in the directory
			string testFilePath = Path.Combine(directoryPath, "test_permission.txt");
			using (FileStream fs = File.Create(testFilePath))
			{
				// If we can create and write to the file, we have write permissions
			}
			File.Delete(testFilePath);
			return true;
		}
		catch
		{
			return false;
		}
	}
}



// Custom CsvDataReader class to bridge CsvHelper and SqlBulkCopy
public class CsvDataReader : IDataReader
{
	private CsvReader _csv;

	public CsvDataReader(CsvReader csv)
	{
		_csv = csv;
	}

	public object GetValue(int i) => _csv.GetField(i);
	public int FieldCount => _csv.HeaderRecord.Length;
	public bool Read() => _csv.Read();

	// Implement GetValues method
	public int GetValues(object[] values)
	{
		int count = Math.Min(values.Length, FieldCount);
		for (int i = 0; i < count; i++)
		{
			values[i] = GetValue(i);
		}
		return count;
	}

	public DataTable GetSchemaTable() => null;
	public bool NextResult() => false;
	public int Depth => 0;
	public bool IsClosed => false;
	public int RecordsAffected => -1;

	public void Close() { }
	public void Dispose() { }
	public string GetName(int i) => _csv.HeaderRecord[i];
	public int GetOrdinal(string name) => Array.IndexOf(_csv.HeaderRecord, name);
	public bool IsDBNull(int i) => string.IsNullOrEmpty(_csv.GetField(i));
	public object this[int i] => GetValue(i);
	public object this[string name] => GetValue(GetOrdinal(name));

	// Other methods (unchanged)
	public bool GetBoolean(int i) => bool.Parse(GetValue(i).ToString());
	public byte GetByte(int i) => byte.Parse(GetValue(i).ToString());
	public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => 0;
	public char GetChar(int i) => char.Parse(GetValue(i).ToString());
	public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => 0;
	public IDataReader GetData(int i) => null;
	public string GetDataTypeName(int i) => typeof(string).Name;
	public DateTime GetDateTime(int i) => DateTime.Parse(GetValue(i).ToString());
	public decimal GetDecimal(int i) => decimal.Parse(GetValue(i).ToString());
	public double GetDouble(int i) => double.Parse(GetValue(i).ToString());
	public Type GetFieldType(int i) => typeof(string);
	public float GetFloat(int i) => float.Parse(GetValue(i).ToString());
	public Guid GetGuid(int i) => Guid.Parse(GetValue(i).ToString());
	public short GetInt16(int i) => short.Parse(GetValue(i).ToString());
	public int GetInt32(int i) => int.Parse(GetValue(i).ToString());
	public long GetInt64(int i) => long.Parse(GetValue(i).ToString());
	public string GetString(int i) => GetValue(i).ToString();
}
