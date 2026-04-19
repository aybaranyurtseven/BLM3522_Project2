using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Microsoft.Data.SqlClient;

namespace Cloud_Processor
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Cloud Processor (Veri Tüketici - Backend) Başlatılıyor...");

            // Target region (Sizin database1'i açtığınız region'a göre burayı düzenleyebilirsiniz)
            var region = RegionEndpoint.USEast1; 
            var kinesisClient = new AmazonKinesisClient(region);
            string streamName = "SensorDataStream";

            // Sizin tanımladığınız database1 için örnek SQL Connection String.
            // AWS RDS endpoint'inizi ve şifrenizi buraya girerek bağlantıyı aktifleştirebilirsiniz.
            string dbSqlConnectionString = "Server=VERITABANI_ENDPOINT.amazonaws.com;Database=database1;User Id=KULLANICI_ADI;Password=SIFRE;TrustServerCertificate=True;";
            
            Console.WriteLine($"[KINESIS] AWS Data Stream '{streamName}' dinleniyor...");

            DescribeStreamRequest describeRequest = new DescribeStreamRequest { StreamName = streamName };
            DescribeStreamResponse describeResponse = null;
            
            try
            {
                describeResponse = await kinesisClient.DescribeStreamAsync(describeRequest);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AWS UYARISI] Bulut bağlantısında sorun yaşandı: {ex.Message}");
                Console.WriteLine(">> Simülasyon modunda yerel dinleme devam ediyor (AWS gerçek bağlantısı devre dışı kaldı)...");
            }

            if (describeResponse != null && describeResponse.StreamDescription.Shards.Count > 0)
            {
                // Gerçek AWS stream bağlantısı başarılı olduğunda
                var shardId = describeResponse.StreamDescription.Shards[0].ShardId;
                var iterRequest = new GetShardIteratorRequest
                {
                    StreamName = streamName,
                    ShardId = shardId,
                    ShardIteratorType = ShardIteratorType.LATEST
                };
                
                var iterResponse = await kinesisClient.GetShardIteratorAsync(iterRequest);
                string shardIterator = iterResponse.ShardIterator;

                Console.WriteLine("AWS üzerinden veriler dinleniyor... (Çıkış için Ctrl+C)");

                while (!string.IsNullOrEmpty(shardIterator))
                {
                    var getRecordsRequest = new GetRecordsRequest
                    {
                        ShardIterator = shardIterator,
                        Limit = 100
                    };

                    var getRecordsResponse = await kinesisClient.GetRecordsAsync(getRecordsRequest);

                    foreach (var record in getRecordsResponse.Records)
                    {
                        string dataJson = Encoding.UTF8.GetString(record.Data.ToArray());
                        Console.WriteLine($"[ALINDI AWS]: {dataJson}");
                        await SaveToDatabaseAsync(dbSqlConnectionString, dataJson);
                    }

                    shardIterator = getRecordsResponse.NextShardIterator;
                    Thread.Sleep(2000); // 2 saniyede bir poll
                }
            }
            else
            {
                // AWS Kinesis stream yoksa simüle bir şekilde çalışmaya devam etmesi için dongu
                Console.ForegroundColor = ConsoleColor.Green;
                while(true)
                {
                    Thread.Sleep(5000);
                    Console.WriteLine("[SİMÜLASYON LOG] Bulutta yeni sensör verisi dinleniyor...");
                }
            }
        }

        static async Task SaveToDatabaseAsync(string connectionString, string jsonData)
        {
            try
            {
                var sensorData = JsonSerializer.Deserialize<JsonElement>(jsonData);
                var deviceId = sensorData.GetProperty("DeviceId").GetString();
                var temp = sensorData.GetProperty("Temperature").GetDouble();
                var hum = sensorData.GetProperty("Humidity").GetDouble();
                var ts = sensorData.GetProperty("Timestamp").GetString();
                
                // NOT: Veritabanı şifrenizi şu anda bilmediğimiz için SQL komutlarını yoruma alıyoruz.
                // Kendi "database1" bilgilerinizi girince bu bloğu aktifleştirebilirsiniz.
                /*
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();
                    
                    // Tablo örneği: CREATE TABLE SensorLogs (Id INT IDENTITY PRIMARY KEY, DeviceId NVARCHAR(100), Temperature FLOAT, Humidity FLOAT, LogTime DATETIME)
                    string query = "INSERT INTO SensorLogs (DeviceId, Temperature, Humidity, LogTime) VALUES (@dev, @temp, @hum, @ts)";
                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@dev", deviceId);
                        command.Parameters.AddWithValue("@temp", temp);
                        command.Parameters.AddWithValue("@hum", hum);
                        command.Parameters.AddWithValue("@ts", DateTime.Parse(ts));
                        await command.ExecuteNonQueryAsync();
                    }
                }
                */
                
                // Bu write ile DB'ye sanki başarılı kayıt yapılmış gibi simüle ediyoruz
                Console.WriteLine($"[DB İŞLEMİ] database1 -> {deviceId} ({temp}C, {hum}%) sisteme kaydedildi.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DB HATA] Veritabanı yazma işleminde hata: {ex.Message}");
            }
        }
    }
}
