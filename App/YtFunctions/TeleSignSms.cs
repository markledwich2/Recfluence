using System;

namespace YtFunctions {
  public class TeleSignSms {
    public string          reference_id     { get; set; }
    public string          sub_resource     { get; set; }
    public SmsStatus       status           { get; set; }
    public DateTime        submit_timestamp { get; set; }
    public SmsUserResponse user_response    { get; set; }

    public class SmsStatus {
      public string   code        { get; set; }
      public string   description { get; set; }
      public DateTime updated_on  { get; set; }
    }

    public class SmsUserResponse {
      public string phone_number     { get; set; }
      public string iso_country_code { get; set; }
      public string sender_id        { get; set; }
      public string mo_message       { get; set; }
    }
  }
}