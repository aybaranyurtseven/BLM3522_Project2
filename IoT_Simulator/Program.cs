using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;

namespace IoT_Simulator
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("IoT Simülatörü Başlatılıyor...");
            
            // Not: AWS Credentials sisteminizin AWS CLI ayarlarından (.aws/credentials) veya ortam değişkenlerinden otomatik çekilir.
            // Target region: Kendinize göre değiştirebilirsiniz (Örn: USEast1, EUCentral1).
            var region = RegionEndpoint.USEast1; 
            var kinesisClient = new AmazonKinesisClient(region);
            
            // AWS üzerinden "SensorDataStream" adında bir Kinesis Data Stream açmanız gerekir.
            string streamName = "SensorDataStream";

            Console.WriteLine($"Kinesis Stream '{streamName}' hedefleniyor...");
            Console.WriteLine("Cihaz sensör verileri (Sıcaklık ve Nem) üretilip gönderilecek.");
            Console.WriteLine("Çıkış için Ctrl+C'ye basın.");

            Random random = new Random();
            int deviceId = random.Next(1, 100);

            while (true)
            {
                var sensorData = new
                {
                    DeviceId = $"Sensor-{deviceId}",
                    Temperature = Math.Round(random.NextDouble() * 15 + 20, 2), // 20.0 - 35.0 C arası
                    Humidity = Math.Round(random.NextDouble() * 20 + 40, 2),    // %40.0 - %60.0 arası
                    Timestamp = DateTime.UtcNow.ToString("o")
                };

                string dataAsJson = JsonSerializer.Serialize(sensorData);
                var dataAsBytes = Encoding.UTF8.GetBytes(dataAsJson);
                
                using (var memoryStream = new System.IO.MemoryStream(dataAsBytes))
                {
                    var putRecordRequest = new PutRecordRequest
                    {
                        StreamName = streamName,
                        PartitionKey = sensorData.DeviceId,
                        Data = memoryStream
                    };

                    try
                    {
                        // Kinesis'e gönderim kodu (Eğer stream henüz yoks hata verir ve simüle modda ekrana basar)
                        var response = await kinesisClient.PutRecordAsync(putRecordRequest);
                        Console.WriteLine($"[GÖNDERİLDİ KINESIS] {dataAsJson} (Seq: {response.SequenceNumber})");
                    }
                    catch (Exception ex)
                    {
                        // Catching general exception handles missing credential errors (AmazonClientException) gracefully
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"[AWS BAĞLANTI DURUMU] {ex.Message}");
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine($"[LOCAL LOG Simülasyon] Veri Üretildi: {dataAsJson}");
                        Console.ResetColor();
                    }
                }

                Thread.Sleep(1000); // Saniyede 1 veri
            }
        }
    }
}
