using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using Npgsql;
using System.Data;
using System.Diagnostics;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.IO;
using System.Threading;

namespace ConsolePressOne
{
    class Services
    {
        string connectionString = "";
        NpgsqlConnection conn;
        HttpClient client = new HttpClient();
        String strHostName = string.Empty;
        //Services()
       // {
            //IPAddress[] addresslist = Dns.GetHostAddresses(Dns.GetHostName());
       // }
        public void OpenConection()
        {
            // Connect to a PostgreSQL database
            connectionString = ConfigurationManager.ConnectionStrings["Default"].ConnectionString;
           
            try
            {
                conn = new NpgsqlConnection(connectionString);
                conn.Open();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.GetBaseException().Message);
            }
            
        }
        public void CloseConnection()
        {
            conn.Close();
        }
        public NpgsqlCommand readQuery(string query)
        {
            return new NpgsqlCommand(query, conn);
        }
        public NpgsqlDataReader DataReader(string Query_)
        {
            NpgsqlCommand cmd = new NpgsqlCommand(Query_, conn);
            NpgsqlDataReader dr = cmd.ExecuteReader();
            return dr;
        }
        public Int64 getActiveCampaignCount()
        {
            OpenConection();
            // Define a query returning a single row result set
            NpgsqlCommand command = new NpgsqlCommand("SELECT COUNT(*) FROM campaign_queues where active = 1", conn);

            // Execute the query and obtain the value of the first column of the first row
            Int64 count = (Int64)command.ExecuteScalar();
            CloseConnection();
            return count;
        }
        public bool isDncs(string phone)
        {
            OpenConection();
            // Define a query returning a single row result set
            NpgsqlCommand command = new NpgsqlCommand("SELECT COUNT(*) FROM dncs where phone = '"+ phone + "' ", conn);

            // Execute the query and obtain the value of the first column of the first row
            Int64 count = (Int64)command.ExecuteScalar();
            CloseConnection();
            if(count > 0)
            {
                return true;
            }
            else
            {
                return false;
            }            
        }
        public void setMaxQueueLastRunCount(string last_run, string cq_id)
        {
            OpenConection();
            // Define a query returning a single row result set
            string updateQry = "update campaign_queues set last_run = " + last_run + " where id =  " + cq_id;
            NpgsqlCommand command = new NpgsqlCommand(updateQry, conn);
           // Console.Write(" {0}\n", updateQry);
            // Execute the query and obtain the value of the first column of the first row
            int cnt = command.ExecuteNonQuery();
          //  Console.Write("Effected Rown Campaigns are {0}\n", cnt);
            CloseConnection();
           // return null;
        }
        public void saveDialedNumbers(Dictionary<string, string> insert)
        {
            OpenConection();
            string dt = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            // Define a query returning a single row result set
            string updateQry = "INSERT INTO campaign_dials (campaign_id, file_id, company_id, phonefrom, transferto, ivrAudio, vmaudio, campaigntype, isvmdrop, dnclistkeypressnumber, transfertokeypressnumber, phoneto, dropid, uuid, provider, created_by, created )";
            updateQry += " values ("+ insert["campaign_id"] + ", " + insert["file_id"] + ", " + insert["company_id"] + ", '" + insert["phonefrom"] + "', '" + insert["transferto"] + "', '" + insert["ivrAudio"] + "', '" + insert["vmaudio"] + "', '" + insert["campaigntype"] + "', '" + insert["isvmdrop"] + "', '" + insert["dnclistkeypressnumber"] + "', '" + insert["transfertokeypressnumber"] + "', '" + insert["phoneto"] + "', '" + insert["dropid"] + "', '" + insert["uuid"] + "', '" + insert["provider"] + "', " + insert["created_by"] + ", '" + dt + "' )";
            NpgsqlCommand command = new NpgsqlCommand(updateQry, conn);
           // Console.Write(" {0}\n", updateQry);
            // Execute the query and obtain the value of the first column of the first row
            int cnt = command.ExecuteNonQuery();
           // Console.Write("Saved Rown Campaigns are {0}\n", cnt);
            CloseConnection();
            // return null;
        }
        public DataSet getCampaignInQueues()
        {
            string campaignQry = "SELECT c.id campaign_id, c.name AS campaign_name,	c.file_id,	c.company_id,	m.phone AS mask, a.path AS ivr_audio, c.dial_type, c.channels , c.agent_count, ck.press_key, ck.action, cs.daily_limit, cs.limits, cs.start_date, cs.start_time, cs.end_date, cs.end_time, cq.last_run, cq.id AS queue_id, cq.started_by";
            campaignQry += " FROM campaigns c ";
            campaignQry += " JOIN campaign_schedules cs ON (c.id = cs.campaign_id) ";
            campaignQry += " JOIN campaign_keys ck ON (c.id = ck.campaign_id) ";
            campaignQry += " JOIN masks m ON (m.id = c.mask_id) ";
            campaignQry += " JOIN audios a ON (a.id = c.ivr_audio_id) ";
            campaignQry += " JOIN campaign_queues cq ON ( c.id = cq.campaign_id and c.company_id = cq.company_id and c.file_id = cq.file_id ) ";
            campaignQry += " WHERE c.active = 1 and cq.active = 1 ";

            Console.Write(" {0}\n", campaignQry);
            DataSet campaignDS = new DataSet();
            NpgsqlDataAdapter campaignDA = new NpgsqlDataAdapter(campaignQry, conn);
            OpenConection();
            campaignDA.Fill(campaignDS, "Campaign");
            CloseConnection();
            return campaignDS;
        }
        public DataSet getContactsForQueueCampaigns(DataRow campaigns )
        {
            string dialType = campaigns["dial_type"].ToString();
            Console.Write("dial type {0}  \n", campaigns["dial_type"].ToString());
            Console.Write("cheee 1 type {0}  \n", campaigns["channels"]);
            // Console.Write("cheee 2 type {0}  \n", (long)campaignRow["channels"]);

            string agent_count = campaigns["agent_count"].ToString();
            string channels = campaigns["channels"].ToString();
            long last_run = (long)campaigns["last_run"];
            if (!dialType.Equals("Max Channels"))
            {
                // channels *= agent_count;
            }
            string contactQry = "select id, phone from contacts where id > "+ campaigns["last_run"] + " and company_id = " + campaigns["company_id"] + " and file_id = " + campaigns["file_id"] + " LIMIT "+ campaigns["channels"]+ " ; ";
            Console.Write(" {0}\n", contactQry);
            DataSet contactDS = new DataSet();
            NpgsqlDataAdapter contactDA = new NpgsqlDataAdapter(contactQry, conn);
            OpenConection();
            contactDA.Fill(contactDS);
            CloseConnection();
            return contactDS;
        }
        public async void callApi(string jsonConvertedString)
        {
            var stringContent = new StringContent(jsonConvertedString, Encoding.UTF8, "application/json");
            
            Uri theUri = new Uri("http://127.0.0.1:8080/call");
            //client.BaseAddress = new Uri("http://127.0.0.1:8080/call");
            // Get the response.
            try
            {
                HttpResponseMessage response = await client.PostAsync("http://127.0.0.1:8080/call", stringContent);

                // Get the response content.
                 HttpContent responseContent = response.Content;

                // Get the stream of the content.
                  using (var reader = new StreamReader(await responseContent.ReadAsStreamAsync()))
                  {
                // Write the output.
                     Console.WriteLine(await reader.ReadToEndAsync());
                 }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.GetBaseException().Message);
            }
        }
        public void SendDialCallThreads()
        {

            Console.Write(" I am in thread \t \n");
            DataSet campaignDS = getCampaignInQueues();
            foreach (DataRow campaignRow in campaignDS.Tables["Campaign"].Rows)//campaignDS.Tables["Customers"].Rows
            {
                // Console.Write("{0}  \n", campaignRow["campaign_id"] + "-" + campaignRow["file_id"] + "-" + campaignRow["company_id"] + "-" + campaignRow["mask"] + "-" + campaignRow["ivr_audio"] + "-" + campaignRow["last_run"] + "-" + campaignRow["dial_type"] + "-" + campaignRow["channels"] + "-" + campaignRow["agent_count"] + "-" + campaignRow["queue_id"]);
                // Console.Write("dial type {0}  \n", campaignRow["dial_type"].ToString());
                string dialType = campaignRow["dial_type"].ToString();
              

                string last_run = campaignRow["last_run"].ToString();
                DataSet contactDS = getContactsForQueueCampaigns(campaignRow);
                foreach (DataRow contactRow in contactDS.Tables[0].Rows)//campaignDS.Tables["Customers"].Rows
                {
                    last_run = contactRow["id"].ToString();
                    if (isDncs(contactRow["phone"].ToString()))
                    {
                        continue;
                    }
                    //Console.Write("Phone {0}\t \n", contactRow["phone"]);
                    //Console.Write("dial queue_id {0}\t \n", campaignRow["queue_id"]);
                    var row = new Dictionary<string, object>();
                    Dictionary<string, string> insertRow = new Dictionary<string, string>();

                    string guuid = Guid.NewGuid().ToString();
                    string[] phone = new string[1];
                    phone[0] = contactRow["phone"].ToString();
                    var phoneContent = new StringContent(JsonConvert.SerializeObject(phone), Encoding.UTF8, "application/json");
                    Console.WriteLine("New Row created.");
                    row["PhoneFrom"] = campaignRow["mask"].ToString();
                    row["TransferTo"] = campaignRow["action"].ToString();
                    row["IvrAudio"] = "http://127.0.0.1:7777/press-one" + campaignRow["ivr_audio"].ToString(); //need path of application
                    row["VmAudio"] = "http://127.0.0.1:7777/press-one" + campaignRow["ivr_audio"].ToString();
                    row["CampaignType"] = "1";//campaignRow["campaign_id"].ToString();
                    row["IsVMDrop"] = "false";
                    row["DncListKeyPressNumber"] = "8";
                    row["TransferToKeyPressNumber"] = campaignRow["press_key"].ToString();
                    row["PhoneTo"] = phone;
                    row["DropId"] = guuid;
                    row["Provider"] = "NavTel";//'telcast';//'telenyx';

                    //    Console.WriteLine("Data Set Mapped.");
                    string myJsonString = JsonConvert.SerializeObject(row);
                    Console.WriteLine(myJsonString);
                    callApi(myJsonString);

                    insertRow["campaign_id"] = campaignRow["campaign_id"].ToString();
                    insertRow["file_id"] = campaignRow["file_id"].ToString();
                    insertRow["company_id"] = campaignRow["company_id"].ToString();
                    insertRow["phonefrom"] = campaignRow["mask"].ToString();
                    insertRow["transferto"] = campaignRow["action"].ToString();
                    insertRow["ivrAudio"] = "http://127.0.0.1:7777/press-one" + campaignRow["ivr_audio"].ToString();
                    insertRow["vmaudio"] = "http://127.0.0.1:7777/press-one" + campaignRow["ivr_audio"].ToString();
                    insertRow["campaigntype"] = campaignRow["campaign_name"].ToString();
                    insertRow["isvmdrop"] = "false";
                    insertRow["dnclistkeypressnumber"] = "8";
                    insertRow["transfertokeypressnumber"] = campaignRow["press_key"].ToString();
                    insertRow["phoneto"] = contactRow["phone"].ToString();
                    insertRow["dropid"] = guuid;
                    insertRow["uuid"] = guuid;
                    insertRow["provider"] = "NavTel";
                    insertRow["created_by"] = campaignRow["started_by"].ToString();

                    setMaxQueueLastRunCount(last_run, campaignRow["queue_id"].ToString());

                    saveDialedNumbers(insertRow);

                }

            }

        }

        public void SendDialCallServices()
        {
            Console.WriteLine("I am in Service Class Function.");
            Int64 count = getActiveCampaignCount();
            if (count > 0)
            {
                Console.Write("Active campaigns are {0}\n", count);

                //private void MyMethod(string param1, int param2)
                //{
                //do stuff
                // }
                // Thread myNewThread = new Thread(() => MyMethod("param1", 5));
                // myNewThread.Start();

                // SendDialCallThreads();
                Thread oThread = new Thread(SendDialCallThreads);
                // SendDialCallThreads(campaignDS);
                //Thread oThread = new Thread(() => SendDialCallThreads(campaignDS));
                oThread.Start();
            }
            else
            {
                Console.WriteLine("Not any campaign running.");
               // Console.Clear();
            }


        }
        public void callApiOldee(string jsonConvertedString)
        {
            HttpClient client = new HttpClient();
            client.BaseAddress = new Uri("http://127.0.0.1:8080/call");
            client.DefaultRequestHeaders
                      .Accept
                      .Add(new MediaTypeWithQualityHeaderValue("application/json"));//ACCEPT header

            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, "relativeAddress");
            request.Content = new StringContent(jsonConvertedString, Encoding.UTF8, "application/json");//CONTENT-TYPE header

            client.SendAsync(request)
                    .ContinueWith(responseTask =>
                    {
                        Console.WriteLine("Response: {0}", responseTask.Result);
                    });

        }
        public DataTable MakeTable(string phone, DataRow campaignRow)
        {
            var table = new DataTable();
            table.TableName = "table";
            // table.Columns.Add("America", typeof(Place));
            // table.Columns.Add("Africa", typeof(Place));
            // table.Columns.Add("Japan", typeof(Place));
            // SELECT c.id campaign_id,	c.file_id,	c.company_id,	m.phone AS mask, a.path AS ivr_audio, c.dial_type, c.channels , c.agent_count, ck.press_key, ck.action, cs.daily_limit, cs.limits, cs.start_date, cs.start_time, cs.end_date, cs.end_time, cq.last_run, cq.id AS queue_id";
            DataRow row = table.NewRow();
            row["PhoneFrom"] = campaignRow["mask"].ToString();
            row["TransferTo"] = campaignRow["action"].ToString();
            row["IvrAudio"] = campaignRow["ivr_audio"].ToString(); //need path of application
            row["VmAudio"] = campaignRow["ivr_audio"].ToString(); 
            row["CampaignType"] = campaignRow["campaign_id"].ToString();
            row["IsVMDrop"] = "false";
            row["DncListKeyPressNumber"] = "8";
            row["TransferToKeyPressNumber"] = campaignRow["press_key"].ToString();
            row["PhoneTo"] = phone;
            row["DropId"] = Guid.NewGuid().ToString();
            row["Provider"] = "NavTel";//'telcast';//'telenyx';

           // table.Print();

            //At the end just add that row in datatable
            //  row["America"]  = JsonConvert.DeserializeObject<Place>(@"{""Id"":1,""Title"":""Ka""}");
            //  row["Africa"]   = JsonConvert.DeserializeObject<Place>(@"{""Id"":2,""Title"":""Sf""}");
            //  row["Japan"]    = JsonConvert.DeserializeObject<Place>(@"{""Id"":3,""Title"":""Ja"",""Values"":{""ValID"":4,""Type"":""Okinawa""}}");
            table.Rows.Add(row);

            foreach (DataRow rows in table.Rows)
            {
                System.IO.StringWriter sw = new System.IO.StringWriter();
                foreach (DataColumn col in table.Columns)
                    sw.Write(rows[col].ToString() + "\t");
                string output = sw.ToString();
                Console.WriteLine(output);
            }

            return table;

        }
        public void SendDialCallServicesjunk()
        {
            Console.WriteLine("I am in Service Class Function.");
            // OpenConection();
            // Define a query returning a single row result set
            // NpgsqlCommand command = new NpgsqlCommand("SELECT COUNT(*) FROM campaign_queues where active = 1", conn);
            // Execute the query and obtain the value of the first column of the first row
            //  Int64 count = (Int64)command.ExecuteScalar();
            // getActiveCampaignCount();
            Int64 count = getActiveCampaignCount();
            if (count > 0)
            {
                Console.Write("Active campaigns are {0}\n", count);

                string contactQry = "select phone from contacts where company_id = @companyId and file_id = @fileId LIMIT @rowCount OFFSET @greaterThen; ";
                NpgsqlCommand cmdGetContacts = new NpgsqlCommand(contactQry, conn);

                // DataSet campaignDS = new DataSet();
                // NpgsqlDataAdapter campaignDA = new NpgsqlDataAdapter(campaignQry, conn);
                // campaignDA.Fill(campaignDS, "Campaign");
                // CloseConnection();
                DataSet campaignDS = getCampaignInQueues();
                foreach (DataRow campaignRow in campaignDS.Tables["Campaign"].Rows)//campaignDS.Tables["Customers"].Rows
                {
                    
                  
                    Console.Write("{0}  \n", campaignRow["campaign_id"] + "-" + campaignRow["file_id"] + "-" + campaignRow["company_id"] + "-" + campaignRow["mask"] + "-" + campaignRow["ivr_audio"] + "-" + campaignRow["last_run"] + "-" + campaignRow["dial_type"] + "-" + campaignRow["channels"] + "-" + campaignRow["agent_count"]);

                    string dialType = campaignRow["dial_type"].ToString();
                    Console.Write("dial type {0}  \n", campaignRow["dial_type"].ToString());
                    Console.Write("cheee 1 type {0}  \n", campaignRow["channels"]);
                   // Console.Write("cheee 2 type {0}  \n", (long)campaignRow["channels"]);



                    string agent_count = campaignRow["agent_count"].ToString();
                    string channels = campaignRow["channels"].ToString();
                    long last_run = (long)campaignRow["last_run"];
                    Console.Write("last run {0}  \n", campaignRow["last_run"]);
                    if (dialType != "Max Channels")
                    {                        
                       // channels *= agent_count;
                    }
                   
                    Console.Write("channels {0}  \n", channels);
                    cmdGetContacts.Parameters.AddWithValue("@companyId", campaignRow["company_id"]);
                    cmdGetContacts.Parameters.AddWithValue("@fileId", campaignRow["file_id"]);
                    cmdGetContacts.Parameters.AddWithValue("@rowCount", campaignRow["channels"]);
                    cmdGetContacts.Parameters.AddWithValue("@greaterThen", campaignRow["last_run"]);


                    NpgsqlDataAdapter contactDA = new NpgsqlDataAdapter();
                    contactDA.SelectCommand = cmdGetContacts;
                    DataSet contactDS = new DataSet();
                    OpenConection();
                    contactDA.Fill(contactDS);
                    CloseConnection();
                    foreach (DataRow contactRow in contactDS.Tables[0].Rows)//campaignDS.Tables["Customers"].Rows
                    {
                        Console.Write("{0}\t \n", contactRow["phone"]);
                    }
                }           
            }
            else
            {
                Console.WriteLine("Not any campaign running.");
            }

           
        }
        public void SendDialCallServicesWay2()
        {
            string campaignQry = "SELECT c.id campaign_id,	c.file_id,	c.company_id,	m.phone AS mask, a.path as ivr_audio, c.dial_type, c.channels , c.agent_count, ck.press_key, ck.action, cs.daily_limit, cs.limits, cs.start_date, cs.start_time, cs.end_date, cs.end_time";
            campaignQry += " FROM campaigns c ";
            campaignQry += " JOIN campaign_schedules cs ON (c.id = cs.campaign_id) ";
            campaignQry += " JOIN campaign_keys ck ON (c.id = ck.campaign_id) ";
            campaignQry += " JOIN masks m ON (m.id = c.mask_id) ";
            campaignQry += " JOIN audios a ON (a.id = c.ivr_audio_id) ";
            campaignQry += " JOIN campaign_queues cq ON ( c.id = cq.campaign_id and c.company_id = cq.company_id and c.file_id = cq.file_id ) ";
            campaignQry += " WHERE c.active = 1 and cq.active = 1 ";


            OpenConection();
            using (NpgsqlDataReader drqry = DataReader(campaignQry))
            {
                // NpgsqlDataReader drqry = DataReader(qry);
                // NpgsqlCommand cmdqry = new NpgsqlCommand(qry, conn);
                // NpgsqlDataReader drqry = cmdqry.ExecuteReader();
                // Execute the query and obtain a result set
                // NpgsqlDataReader dr = command.ExecuteReader();
                if (drqry.HasRows)
                {
                    while (drqry.Read())
                    {
                        Console.Write("{0}\t{1}\t{2}\t{3}\t{4}  \n", drqry[0], drqry[1], drqry[2], drqry[3], drqry[4]);

                        // var dictionary = new Dictionary<string, string>();
                        // dictionary.Add("campaign_id", "value1");
                        // Console.Write("{0}\t{1}\t{2}\t{3}\t{4}  \n", drqry["campaign_id"], drqry["file_id"], drqry["company_id"], drqry["mask"], drqry["ivr_audio"]);

                        string qry2 = "select phone from contacts where company_id = " + drqry["company_id"] + " and file_id = " + drqry["file_id"] + "; ";
                        Console.WriteLine(qry2);
                        using (NpgsqlDataReader drqry2 = DataReader(qry2))
                        {
                            // NpgsqlDataReader drqry2 = DataReader(qry2);
                            Console.WriteLine(drqry2);
                            //NpgsqlCommand cmdqry2 = new NpgsqlCommand(qry2, conn);
                            //NpgsqlDataReader drqry2 = cmdqry2.ExecuteReader();
                            while (drqry2.Read())
                            {
                                Console.Write("{0}\t \n", drqry2[0]);
                            }
                        }

                    }
                    //  drqry.NextResult();
                }
                else
                {
                    Console.WriteLine("no active campaign related to active queue.");
                }
                drqry.Close();
            }
            CloseConnection();
        }
        public void SendDialCallServicesWay3()
        {
            string campaignQry = "SELECT c.id campaign_id,	c.file_id,	c.company_id,	m.phone AS mask, a.path as ivr_audio, c.dial_type, c.channels , c.agent_count, ck.press_key, ck.action, cs.daily_limit, cs.limits, cs.start_date, cs.start_time, cs.end_date, cs.end_time";
            campaignQry += " FROM campaigns c ";
            campaignQry += " JOIN campaign_schedules cs ON (c.id = cs.campaign_id) ";
            campaignQry += " JOIN campaign_keys ck ON (c.id = ck.campaign_id) ";
            campaignQry += " JOIN masks m ON (m.id = c.mask_id) ";
            campaignQry += " JOIN audios a ON (a.id = c.ivr_audio_id) ";
            campaignQry += " JOIN campaign_queues cq ON ( c.id = cq.campaign_id and c.company_id = cq.company_id and c.file_id = cq.file_id ) ";
            campaignQry += " WHERE c.active = 1 and cq.active = 1 ";


            OpenConection();
            using (NpgsqlDataReader drqry = DataReader(campaignQry))
            {
                DataTable schemaTable = drqry.GetSchemaTable();

                foreach (DataRow row in schemaTable.Rows)
                {
                    foreach (DataColumn column in schemaTable.Columns)
                    {
                        Console.WriteLine(String.Format("{0} = {1}",
                           column.ColumnName, row[column]));
                    }
                }
                // NpgsqlDataReader drqry = DataReader(qry);
                // NpgsqlCommand cmdqry = new NpgsqlCommand(qry, conn);
                // NpgsqlDataReader drqry = cmdqry.ExecuteReader();
                // Execute the query and obtain a result set
                // NpgsqlDataReader dr = command.ExecuteReader();
                if (drqry.HasRows)
                {
                    while (drqry.Read())
                    {
                        Console.Write("{0}\t{1}\t{2}\t{3}\t{4}  \n", drqry[0], drqry[1], drqry[2], drqry[3], drqry[4]);

                        // var dictionary = new Dictionary<string, string>();
                        // dictionary.Add("campaign_id", "value1");
                        // Console.Write("{0}\t{1}\t{2}\t{3}\t{4}  \n", drqry["campaign_id"], drqry["file_id"], drqry["company_id"], drqry["mask"], drqry["ivr_audio"]);

                        string qry2 = "select phone from contacts where company_id = " + drqry["company_id"] + " and file_id = " + drqry["file_id"] + "; ";
                        Console.WriteLine(qry2);
                        using (NpgsqlDataReader drqry2 = DataReader(qry2))
                        {
                            // NpgsqlDataReader drqry2 = DataReader(qry2);
                            Console.WriteLine(drqry2);
                            //NpgsqlCommand cmdqry2 = new NpgsqlCommand(qry2, conn);
                            //NpgsqlDataReader drqry2 = cmdqry2.ExecuteReader();
                            while (drqry2.Read())
                            {
                                Console.Write("{0}\t \n", drqry2[0]);
                            }
                        }

                    }
                    //  drqry.NextResult();
                }
                else
                {
                    Console.WriteLine("no active campaign related to active queue.");
                }
                drqry.Close();
            }
            CloseConnection();
        }
        public void SendDialCallServicesWithoutThread()
        {
            Console.WriteLine("I am in Service Class Function.");
            // OpenConection();
            // Define a query returning a single row result set
            // NpgsqlCommand command = new NpgsqlCommand("SELECT COUNT(*) FROM campaign_queues where active = 1", conn);
            // Execute the query and obtain the value of the first column of the first row
            //  Int64 count = (Int64)command.ExecuteScalar();
            // getActiveCampaignCount();
            Int64 count = getActiveCampaignCount();
            if (count > 0)
            {
                Console.Write("Active campaigns are {0}\n", count);

                //  string contactQry = "select phone from contacts where company_id = @companyId and file_id = @fileId LIMIT @rowCount OFFSET @greaterThen; ";
                //  NpgsqlCommand cmdGetContacts = new NpgsqlCommand(contactQry, conn);

                //  DataSet campaignDS = new DataSet();
                //  NpgsqlDataAdapter campaignDA = new NpgsqlDataAdapter(campaignQry, conn);
                //  campaignDA.Fill(campaignDS, "Campaign");
                //  CloseConnection();

                DataSet campaignDS = getCampaignInQueues();
                foreach (DataRow campaignRow in campaignDS.Tables["Campaign"].Rows)//campaignDS.Tables["Customers"].Rows
                {
                    Console.Write("{0}  \n", campaignRow["campaign_id"] + "-" + campaignRow["file_id"] + "-" + campaignRow["company_id"] + "-" + campaignRow["mask"] + "-" + campaignRow["ivr_audio"] + "-" + campaignRow["last_run"] + "-" + campaignRow["dial_type"] + "-" + campaignRow["channels"] + "-" + campaignRow["agent_count"] + "-" + campaignRow["queue_id"]);

                    string dialType = campaignRow["dial_type"].ToString();
                    Console.Write("dial type {0}  \n", campaignRow["dial_type"].ToString());

                    //var objType = JArray.FromObject(campaignRow, JsonSerializer.CreateDefault(new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore })).FirstOrDefault(); // Get the first row            
                    //var js = objType.ToString();

                    // Console.WriteLine(js);
                    //  Debug.WriteLine(js);

                    //   string js = JsonConvert.SerializeObject(campaignRow);
                    //   Console.Write("cheee 1 type {0}  \n", campaignRow["channels"]);                   
                    //   Console.Write("last run {0}  \n", campaignRow["last_run"]);
                    // DataRow campaignRow2 = campaignRow;
                    string last_run = campaignRow["last_run"].ToString();
                    //  DataTable tbl;
                    DataSet contactDS = getContactsForQueueCampaigns(campaignRow);
                    foreach (DataRow contactRow in contactDS.Tables[0].Rows)//campaignDS.Tables["Customers"].Rows
                    {
                        last_run = contactRow["id"].ToString();
                        if (isDncs(contactRow["phone"].ToString()))
                        {
                            continue;
                        }
                        Console.Write("Phone {0}\t \n", contactRow["phone"]);
                        Console.Write("dial queue_id {0}\t \n", campaignRow["queue_id"]);
                        var row = new Dictionary<string, object>();
                        Dictionary<string, string> insertRow = new Dictionary<string, string>();

                        //   Console.WriteLine("Table created.");
                        // table.TableName = "table";
                        // table.Columns.Add("America", typeof(Place));
                        // table.Columns.Add("Africa", typeof(Place));
                        // table.Columns.Add("Japan", typeof(Place));
                        // SELECT c.id campaign_id,	c.file_id,	c.company_id,	m.phone AS mask, a.path AS ivr_audio, c.dial_type, c.channels , c.agent_count, ck.press_key, ck.action, cs.daily_limit, cs.limits, cs.start_date, cs.start_time, cs.end_date, cs.end_time, cq.last_run, cq.id AS queue_id";
                        string guuid = Guid.NewGuid().ToString();
                        string[] phone = new string[1];
                        phone[0] = contactRow["phone"].ToString();
                        var phoneContent = new StringContent(JsonConvert.SerializeObject(phone), Encoding.UTF8, "application/json");
                        Console.WriteLine("New Row created.");
                        row["PhoneFrom"] = campaignRow["mask"].ToString();
                        row["TransferTo"] = campaignRow["action"].ToString();
                        row["IvrAudio"] = "http://127.0.0.1:7777/press-one" + campaignRow["ivr_audio"].ToString(); //need path of application
                        row["VmAudio"] = "http://127.0.0.1:7777/press-one" + campaignRow["ivr_audio"].ToString();
                        row["CampaignType"] = "1";//campaignRow["campaign_id"].ToString();
                        row["IsVMDrop"] = "false";
                        row["DncListKeyPressNumber"] = "8";
                        row["TransferToKeyPressNumber"] = campaignRow["press_key"].ToString();
                        row["PhoneTo"] = phone;
                        row["DropId"] = guuid;
                        row["Provider"] = "NavTel";//'telcast';//'telenyx';

                        //    Console.WriteLine("Data Set Mapped.");
                        string myJsonString = JsonConvert.SerializeObject(row);
                        Console.WriteLine(myJsonString);
                        callApi(myJsonString);

                        insertRow["campaign_id"] = campaignRow["campaign_id"].ToString();
                        insertRow["file_id"] = campaignRow["file_id"].ToString();
                        insertRow["company_id"] = campaignRow["company_id"].ToString();
                        insertRow["phonefrom"] = campaignRow["mask"].ToString();
                        insertRow["transferto"] = campaignRow["action"].ToString();
                        insertRow["ivrAudio"] = "http://127.0.0.1:7777/press-one" + campaignRow["ivr_audio"].ToString();
                        insertRow["vmaudio"] = "http://127.0.0.1:7777/press-one" + campaignRow["ivr_audio"].ToString();
                        insertRow["campaigntype"] = campaignRow["campaign_name"].ToString();
                        insertRow["isvmdrop"] = "false";
                        insertRow["dnclistkeypressnumber"] = "8";
                        insertRow["transfertokeypressnumber"] = campaignRow["press_key"].ToString();
                        insertRow["phoneto"] = contactRow["phone"].ToString();
                        insertRow["dropid"] = guuid;
                        insertRow["uuid"] = guuid;
                        insertRow["provider"] = "NavTel";
                        insertRow["created_by"] = campaignRow["started_by"].ToString();

                        setMaxQueueLastRunCount(last_run, campaignRow["queue_id"].ToString());

                        saveDialedNumbers(insertRow);

                        // string myJsonString2 = JsonConvert.SerializeObject(insertRow);
                        // Console.WriteLine(myJsonString2);
                        //string myJsonString = (new JavaScriptSerializer()).Serialize(row);
                        // table.Print();

                        //At the end just add that row in datatable
                        //  row["America"]  = JsonConvert.DeserializeObject<Place>(@"{""Id"":1,""Title"":""Ka""}");
                        //  row["Africa"]   = JsonConvert.DeserializeObject<Place>(@"{""Id"":2,""Title"":""Sf""}");
                        //  row["Japan"]    = JsonConvert.DeserializeObject<Place>(@"{""Id"":3,""Title"":""Ja"",""Values"":{""ValID"":4,""Type"":""Okinawa""}}");

                        // tbl = MakeTable(contactRow["phone"].ToString(), campaignRow);
                    }

                }
            }
            else
            {
                Console.WriteLine("Not any campaign running.");
                // Console.Clear();
            }


        }
    }
}
