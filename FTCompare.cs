/*                                                             
 Copyright (c) 1999 - 2012 by EFT Technologies, Inc.
 All rights reserved.

 This Software is confidential and proprietary to 
 EFT Technologies and it is protected by U.S. copyright law, 
 other national copyright laws, and international treaties.
 The Software may not be disclosed or reproduced in whole or in part in any manner to 
 any third party without the express prior written consent of 
 EFT Technologies, Inc.                                      
                                                                
 This Software and its related Documentation are proprietary
 and confidential material of EFT Technologies, Inc.
*/
/*

V1.1   Old version not(multi queues) of FTCompare with fld expression added.
c:\\Simulator\\" + m_Area + "\\feed\\fldexprs.xml
This file contains fields of non structual fields like swfmids f20 or msg freetext
to skp  or compare differently.
V1.2   make sure the m_tablelist does not have tables with ALL include fields = "n"
V1.3   add support to compare xml string it is for RBCS handoff.
V1.4   add support for record delimiter and structured record like GIW
V1.5   add support for field aliases. 
v1.6   1. add support for FLX fields translation. 
	  To implement: Place a file named "Table_name"+_fldNames.txt ( Exa: swf_excho$flx_fldNames.txt) into ../feed dir.
	  it should contain a list of the field names for this particular interface.
	  If not file we name the fields in the report as fld1, fld12 etc.
       2. change Newjournal reporting to list ONLY different and missing entries. 
          It takes care of the syncing the entries.
          The same is for msgerr table.
1-15-2015	Several fixes to make DoFieldTranslation routine work. 
1-23-2015	Fixed trap for nbewjournal and msgerr when no entries in DB.


 *      JR - the StringArray constructor needs to know the area. I changed the call to
 *      DoCompare to include the area and then passed that into the StringArray constructor.
 * 28-Nov-15	Synch code between Jacob and John

*/
using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Data;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Xml;
using Simulator.DBLibrary;
using System.Data.SqlClient;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using Simulator.BackEndSubLib;
using Simulator.StringArrayCompare;
using Simulator.SimLog;
using Simulator.EventLogger;

namespace Simulator.FTCompare
{
    public class FTCompareCl
    {
        private DBAccess m_Connection;
        private DBAccess m_ReadConnection;
        //	private OracleConnection m_OraConnection;

        private string m_Area;
        //private string m_Debug;
        //private string m_RootDir;
        static private bool m_NoTemplate;
        static private string m_KeyTabLoaded;
        static private ArrayList m_TableList;
        static private Hashtable m_Keys;
        static private Hashtable m_TableSql;
        static private Hashtable m_FldExprs;
        static private Dictionary<string, List<string>> m_FldAlias;
        static private Dictionary<string, Hashtable> m_FldTranslation;
        static private IDictionary<string, DataTable> m_Templates;
        static private DateTime m_lastUpd;
        static private string m_FldName;
        static private Hashtable m_SrcXmlArr;
        static private Hashtable m_TrgXmlArr;
        private SimulatorLog _simLog = null;
        bool Show, Show1, ShowTime, ShowRes, UpdDb;
        private Hashtable m_Totals;
        private string m_Mid1;
        private string m_Mid2;
        private string m_Batch;
        private string m_DiffUpd;
        private int m_UpdNo;
        int NoDiff;
        private class JournalEntry
        {
            public string Fname { get; set; }
            public string Value { get; set; }
        }

        private class JournalDiffs
        {
            public string Fname { get; set; }
            public string SrcValue { get; set; }
            public string TrgValue { get; set; }
        }

        public int Compare(bool Shw, string Batch, string CmpTable, string Area, string Mid1, string Mid2, string ConnStr1, string ConnStr2, string Db1, string Db2, string Prefix)
        {
            m_Mid1 = Mid1;
            m_Mid2 = Mid2;
            m_Batch = Batch;
            m_NoTemplate = false;
            m_UpdNo = 0;
            m_DiffUpd = "";
            UpdDb = true;
            Show = Shw;
            Show1 = false;
            ShowTime = true;
            ShowRes = false;
            DateTime dt1 = DateTime.MinValue, dt11 = DateTime.MinValue, dt2 = DateTime.MinValue, dt21 = DateTime.MinValue, dt3 = DateTime.MinValue, dt31 = DateTime.MinValue;

            string Sha = System.Environment.GetEnvironmentVariable("SIMSHOWALL");
            if (Sha == "Y")
                Show = true;
            string Sht = System.Environment.GetEnvironmentVariable("SIMSHOWTIME");
            if (Sht == null)
                ShowTime = false;

            if (ShowTime)
            {
                dt3 = DateTime.Now;
                Console.WriteLine("Compare starts for" + Mid1 + "/" + Mid2 + " at " + DateTime.Now.ToString("HH:mm:ss.ffff"));
            }
            ArrayList MapsSrc;
            ArrayList MapsTrg;
            m_Area = Area;
            bool OraDB1 = false;
            bool OraDB2 = false;
            _simLog = new SimulatorLog("FTFeeder");
            NoDiff = -1;

            m_Connection = new DBAccess();
            m_Connection.Connect(true, m_Area);
            /*
            1. Load Key Table m_Keys
            2. Get distinct Tables from XmlSet of it. m_TableList
            3. Create an array of maps for each one
            4. Load maps 
            */

            if (Db1 == "ORA")
                OraDB1 = true;
            else
                OraDB1 = false;

            if (Db2 == "ORA")
                OraDB2 = true;
            else
                OraDB2 = false;

            m_ReadConnection = new DBAccess();
            m_ReadConnection.Connect(false, m_Area);
            try
            {
                if (m_KeyTabLoaded == null)
                    m_KeyTabLoaded = "";
            }
            catch
            {
                Console.WriteLine("HERE");
            }


            LoadKeys(CmpTable);
            LoadTabSql();
            LoadFldExprs();
            LoadTemplates();
            LoadFldAliases();

            try { m_Totals.Clear(); }
            catch { };  // first time it does not exist
            m_Totals = new Hashtable();
            foreach (string a in m_TableList)
                m_Totals.Add(a, 0);

            // Map required Mids
            MapsSrc = new ArrayList();
            MapsTrg = new ArrayList();

            if (ShowTime)
            {
                dt2 = DateTime.Now;
                Console.WriteLine("   Mapping of data starts for " + Mid1 + " - " + Mid2 + " at " + DateTime.Now.ToString("HH:mm:ss.ffff"));
            }
            foreach (string tbl in m_TableList)
            {
                if (Show)
                    Console.WriteLine("Source Load from table - " + Prefix + tbl);
                IDictionary<String, String> map = GetData(Prefix, tbl, Mid1, OraDB1, ConnStr1);
                MapsSrc.Add(map);
                if (ShowTime)
                {
                    dt1 = DateTime.Now;
                    Console.WriteLine("      Source mapping ends for " + tbl + " at " + DateTime.Now.ToString("HH:mm:ss.ffff"));
                }
                //  and Target data
                if (Show)
                    Console.WriteLine("Target Load from table - " + tbl);
                IDictionary<String, String> map1 = GetData("", tbl, Mid2, OraDB2, ConnStr2);
                MapsTrg.Add(map1);
                if (ShowTime)
                {
                    dt11 = DateTime.Now;
                    TimeSpan span = dt11 - dt1;
                    int ms = (int)span.TotalMilliseconds;
                    Console.WriteLine("      Target mapping ends for " + tbl + " at " + DateTime.Now.ToString("HH:mm:ss.ffff") + " - " + ms);
                }
            }
            if (Show)
            {
                int i = 0;
                foreach (IDictionary<String, String> map in MapsSrc)
                {
                    try
                    {
                        foreach (KeyValuePair<String, String> a in map)
                            Console.WriteLine("Source Data After Loading mid key - " + a.Key + "  val - " + a.Value);
                        i++;
                    }
                    catch (Exception ex) // This is OK. Sometimes msg is not in the table. Swfmids ONLY outbound are checked.
                    {
                        Console.WriteLine("Msg " + Mid1 + " is not in source table - " + m_TableList[i] + " error-" + ex.Message);
                    }
                }

                i = 0;
                foreach (IDictionary<String, String> map in MapsTrg)
                {
                    try
                    {
                        foreach (KeyValuePair<String, String> a in map)
                            Console.WriteLine("Target Data After Loading mid key - " + a.Key + "  val - " + a.Value);
                        i++;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Msg " + Mid2 + " is not in target table - " + m_TableList[i] + " error-" + ex.Message);
                    }
                }
            }

            // Do Compare
            if (ShowTime)
            {
                dt21 = DateTime.Now;
                TimeSpan span = dt21 - dt2;
                int ms = (int)span.TotalMilliseconds;
                Console.WriteLine("    Mapping ended. Compare Mids for" + Mid1 + "/" + Mid2 + " at " + DateTime.Now.ToString("HH:mm:ss.ffff") + " - " + ms);
            }

            DoCompare(MapsSrc, MapsTrg, Area);
            if ((UpdDb) && (m_UpdNo > 0))
            {
                m_Connection.Execute(m_DiffUpd, true);
                m_DiffUpd = "";
                m_UpdNo = 0;
            }

            NoDiff = 0;
            int Totmif = 0;
            int Totmtf1000 = 0;
            int Totswfmids = 0;
            int Totfreemsg = 0;
          //  int TotNewjournal = 0;
            int TotOther = 0;


            //Clean up

            MapsSrc.Clear();
            MapsTrg.Clear();

            List<string> keys = new List<string>();
            foreach (DictionaryEntry de in m_Totals)
            {
                keys.Add(de.Key.ToString());
                switch (de.Key.ToString())
                {
                    case "minf":
                    case "mif":
                        Totmif = (int)de.Value;
                        break;
                    case "mtf1000":
                    case "msgerr":  //msgerr
                        Totmtf1000 = (int)de.Value;
                        break;
                    case "swfmids":
                    case "message_external_interaction": 
                        Totswfmids = (int)de.Value;
                        break;
                    case "messagefreetext":
                    case "newjournal": //
                        Totfreemsg = (int)de.Value;
                        break;
                    default:
                        TotOther = (int)de.Value;
                        break;
                }
            }

            foreach (string key in keys)
            {
                NoDiff = NoDiff + (int)m_Totals[key];
                m_Totals[key] = 0;
            }
            //  Update diffsummary
            if ((UpdDb) && (NoDiff > 0))
            {
                m_DiffUpd = string.Format("insert into DiffSummary values ('{0}','{1}','{2}', {3},{4},{5},{6},{7} )", m_Mid1, m_Mid2, m_Batch, NoDiff, Totmif, Totmtf1000, Totswfmids, Totfreemsg);
                m_Connection.Execute(m_DiffUpd, true);
            }
            if (ShowTime)
            {
                dt31 = DateTime.Now;
                TimeSpan span = dt31 - dt3;
                int ms = (int)span.TotalMilliseconds;
                Console.WriteLine("    Processing ended for" + Mid1 + "/" + Mid2 + " - " + ms);
            }
            m_Connection.DisConnect();
            m_ReadConnection.DisConnect();
            return NoDiff;
        }


        public IDictionary<String, String> GetData(string Prefix, string tbl, string mid, bool isOracleDb, string _connectionString)
        {
            IDictionary<String, String> map = new Dictionary<String, String>();
            string Provider;

            if (isOracleDb)
                Provider = "Oracle.ManagedDataAccess.Client";
            else
                Provider = "System.Data.SqlClient";

            DbProviderFactory factory = DbProviderFactories.GetFactory(Provider);
            string dbCommand;

            string tbl1 = tbl;
            if (Prefix.Length != 0)
                tbl1 = Prefix + tbl;

            if (tbl.IndexOf("$") != -1) // one table multiple sqls
            {
                tbl1 = tbl.Substring(0, tbl.IndexOf("$"));
                if (Prefix.Length != 0)
                    tbl1 = Prefix + tbl.Substring(0, tbl.IndexOf("$"));
            }

            //				
            //  IMPORTANT!!!!!!!     WHEN CHANGING SELECT STATEMENT MAKE SURE THAT PK_* IS THERE.
            //
		string cm = "";
		try { cm = m_TableSql[tbl].ToString(); }
		catch { cm = ""; };
		if (cm.Length > 0)
					dbCommand = cm.Replace("table", tbl1).Replace("{0}", mid);
		    else
					dbCommand = string.Format("select * from {0} where mid='{1}'", tbl1, mid);
            if (ExecuteCommand(factory, map, _connectionString, dbCommand, tbl))
                return map;
            return null;
        }

        string dofldexp(string fname)
        {
            string fldexp = "";
            try
            {
                if (Show)
                    Console.WriteLine("before CHECKING  Field Expression for  " + fname);
                fldexp = m_FldExprs[fname].ToString();
                if (Show)
                    Console.WriteLine("Field Expression for  " + fname + " - " + fldexp);
            }
            catch { };
            return fldexp;
        }

        bool ExecuteCommand(DbProviderFactory factory, IDictionary<String, String> map, string connectionString, string dbCommand, string Tbl)
        {
            bool returnStatus = false;
            try
            {
                using (IDbConnection trnListConn = factory.CreateConnection())
                {
                    trnListConn.ConnectionString = connectionString;
                    using (IDbCommand command = factory.CreateCommand())
                    {
                        command.CommandText = dbCommand;
                        trnListConn.Open();
                        command.Connection = trnListConn;
                        using (IDataReader reader = command.ExecuteReader())
                        {
                            int PkNo = 0;
                            int msgi = 0;
                            string msgfname = "";
                            string msgval = "";
                            string nm1 = "";
                            string nt = "";
                            while (reader.Read())
                            {
                                int fieldCount = reader.FieldCount;
                                for (int index = 0; index < fieldCount; index++)
                                {
                                    if (( string.Format("pk_{0}", Tbl.ToLower()).Contains(reader.GetName(index).ToLower()) ) ||
                                         (Tbl == "msg_rule_log"))
                                        PkNo++;

                                    string nm = Tbl + "." + reader.GetName(index).ToLower();
                                    if (m_Keys.Contains(nm))
                                    {
                                        if (Tbl.ToLower() == "messagefreetext")
                                        {
                                            if (msgi == 0)
                                                nt = nm; // keep the correct name for the last entry
                                            if (msgi == 1)
                                                nm1 = nt + "." + msgfname;  // to map the last entry below
                                            if (msgi == 2)
                                            {
                                                msgi = 0;
                                                map[nm + "." + msgfname] = msgval;
                                            }

                                            if (reader.GetName(index).ToLower() == "fieldname")
                                                msgfname = reader[index].ToString();
                                            if (reader.GetName(index).ToLower() == "contents")
                                                msgval = reader[index].ToString();
                                            msgi++;
                                        }
                                        else
                                            map[nm + "." + string.Format("{0:000}", PkNo)] = reader[index].ToString();
                                    }
                                }
                                returnStatus = true;
                            }
                            reader.Close();
                            reader.Dispose();
                            if (Tbl.ToLower() == "messagefreetext") // not to miss the last entry
                            {
                                map[nm1] = msgval;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                string Errstr = string.Format("FT Compare - Error accessing data  - " + ex.Message + " DbComamnd - " + dbCommand);
                ReportError(Errstr, 2);
                if (Show)
                    Console.Write("Error accessing data - " + ex.Message + " DbComamnd - " + dbCommand + " \n");
            }
            return returnStatus;
        }

        private void DoCompare(ArrayList Source, ArrayList Target, string Area)
        {
            // Example	see at the end
            if (Show)
                Console.WriteLine("Start doing Compare");
            for (int i = 0; i < Source.Count; i++)
            {
                if (ShowTime)
                    Console.WriteLine("    Compare table " + m_TableList[i] + " at " + DateTime.Now.ToString("HH:mm:ss.ffff"));
                if (Show)
                    Console.WriteLine("Comparing - " + m_TableList[i]);
                IDictionary<String, String> a = (IDictionary<String, String>)Source[i];
                IDictionary<String, String> b = (IDictionary<String, String>)Target[i];

                if ((a == null) && (b != null))
                {
                    ReportDiffs(m_TableList[i].ToString(), "All Fields", "Fields are missing", " ");
                }
                if ((b == null) && (a != null))
                {
                    ReportDiffs(m_TableList[i].ToString(), "All Fields", " ", "Fields are missing");
                }

                string seq_compTable = "";
                if ((m_TableList[i].ToString().ToLower() == "newjournal") ||
                    (m_TableList[i].ToString().ToLower() == "msgerr"))
                    seq_compTable = m_TableList[i].ToString().ToLower();

                if (m_TableList[i].ToString().ToLower() == seq_compTable)
                {
                    if ((a == null) && (b == null)) // no entries for both seqs
                    {
                        if (Show)
                            Console.WriteLine("No entries for " + seq_compTable);
                        continue;
                    }
                    var NewJournalEntries1 = from val in a
                                             where val.Key.ToLower().StartsWith(seq_compTable)
                                             select new JournalEntry() { Fname = val.Key, Value = val.Value };
                    List<JournalEntry> LstSrc = NewJournalEntries1.ToList();

                    var NewJournalEntries2 = from val in b
                                             where val.Key.ToLower().StartsWith(seq_compTable)
                                             select new JournalEntry() { Fname = val.Key, Value = val.Value };
                    List<JournalEntry> LstTrg = NewJournalEntries2.ToList();
                    foreach (var entry in DiffList(LstSrc, LstTrg))
                    {
                        ReportDiffs(m_TableList[i].ToString(), entry.Fname, entry.SrcValue, entry.TrgValue);
                        //Console.WriteLine("{0} {1} {2}", entry.Fname, entry.SrcValue, entry.TrgValue);
                    }

                    continue;
                }
                if ((b != null) && (a != null))
                {
                    try
                    {
                    	CompPairElem (i, a, b, Area);
                    }
                    catch (Exception ex)
                    {
                        string Errstr = string.Format("FT Compare - Msg {0}/{1} is not in target table - {2} Error: {3}", m_Mid1, m_Mid2, m_TableList[i].ToString(), ex.Message);
                        ReportError(Errstr, 2);
                        if (Show)
                            Console.WriteLine("Msg is not in target table - " + m_TableList[i].ToString());
                    }
                }	//end foreach
            }
        }
        
        private void CompPairElem (int i, IDictionary<String, String> a, IDictionary<String, String> b, string Area)
        {
					foreach (KeyValuePair<String, String> entry in a)
					{			
				    if (!b.ContainsKey(entry.Key)) // Target does not have a key  
				    {
							if (dofldexp(entry.Key) != "SKIP")
						    ReportDiffs(m_TableList[i].ToString(), entry.Key, entry.Value, "Field is missing");
				    }
				    else
				    {
								string keyst = entry.Key.ToString().Substring(0, entry.Key.Length - 4);
								string compstr = "=";
								try { compstr = m_Keys[keyst].ToString(); }
								catch { };
		
					// check if the field is in fldexprs table and override compstr
								try
								{
							    if (Show)
										Console.WriteLine("CHECKING  Field Expression for  " + entry.Key);
							    compstr = m_FldExprs[entry.Key].ToString();
							    if (Show)
										Console.WriteLine("Field Expression for  " + entry.Key + " - " + compstr);
								}
								catch { };
					//Evaluate compstr.
								string xmlNode = "";
								string TabName = "";
								string DlmCr = "";
								string srcxml = "";
								string trgxml = "";
								if ((compstr.Length > 3) && (compstr.Substring(0, 3) == "XML"))
								{
							    xmlNode = compstr.Substring(3);
							    compstr = "XML";
								}
								if ((compstr.Length > 9) && (compstr.Substring(0, 9) == "DELIMITER"))
								{
								    DlmCr = compstr.Substring(10);
								    compstr = "DELIMITER";
								}
								if ((compstr.Length > 8) && (compstr.Substring(0, 8)) == "TEMPLATE")  // format should be TEMPLATE:GIW
								{
								    TabName = compstr.Substring(9);
								    compstr = "TEMPLATE";
								}	
		
								switch (compstr)
								{
							    case "=":
										if (entry.Value.Trim() != b[entry.Key].Trim())
										{
										    if (!FindAlias(m_TableList[i].ToString(), entry.Key, entry.Value, b[entry.Key]))
										    {
													if (Show)
													{
												    string v1 = entry.Value;
												    string v2 = b[entry.Key];
												    Console.WriteLine("Val1 - " + v1 + " v1len - " + v1.Length + " v2- " + v2 + " v2len- " + v2.Length);
													}
													if ( (m_TableList[i].ToString() == "message_external_interaction") ||
															 (m_TableList[i].ToString() == "swfmids") ||
													     (m_TableList[i].ToString().Contains("tags")) )
											    		ShowTags(m_TableList[i].ToString(), entry.Key, entry.Value, b[entry.Key]);
													else
											    	ReportDiffs(m_TableList[i].ToString(), entry.Key, entry.Value, b[entry.Key]);
										    }
										}
										break;
					    //support for delimiters in here
							    case "DELIMITER":
										srcxml = "<OurXml><dlmtxt>";
										trgxml = "<OurXml><dlmtxt>";
										string[] flds = entry.Value.Trim().Split(DlmCr.ToCharArray());
										int fp = 1;
										foreach (string fl in flds)
										{
										    if (fl.Trim().Length > 0)
											srcxml = srcxml + "<fld" + fp + ">" + fl.Trim() + "</fld" + fp + ">";
										    fp++;
										}
										srcxml = srcxml + "</dlmtxt></OurXml>";
										if (Show)
										    Console.WriteLine(fp + " - srcxml - " + srcxml);
		
										flds = b[entry.Key].Trim().Split(DlmCr.ToCharArray());
						// add actual field name if available instead of fldn
										fp = 1;
										foreach (string fl in flds)
										{
										    if (fl.Trim().Length > 0)
											trgxml = trgxml + "<fld" + fp + ">" + fl.Trim() + "</fld" + fp + ">";
										    fp++;
										}
										trgxml = trgxml + "</dlmtxt></OurXml>";
										if (Show)
										    Console.WriteLine(fp + " - trgxml -" + trgxml);
						//  and do compare like in XML below....
										try
										{
										    StringArrayCompare.StringArrayCompare sCompare = new Simulator.StringArrayCompare.StringArrayCompare(Area);
										    string NdName = "/OurXml/dlmtxt";
										    ArrayList results = sCompare.compareMsg(srcxml, trgxml, false, m_Area, NdName);
										    for (int rIdx = 0; rIdx < results.Count; rIdx++)
										    {
													object[] resultsObj = (object[])results[rIdx];
													string xmlSet = (string)resultsObj[0];
											// v 1.6 see if we have a file to translate fldnn to a real name
													xmlSet = DoFieldTranslation(m_TableList[i].ToString(), xmlSet);
													Simulator.StringArrayCompare.diffResult diff = (Simulator.StringArrayCompare.diffResult)resultsObj[1];
													for (int k = 0; k < diff.before.Count; k++)
													{
													    if ((diff.changedFlag[k].ToString() == "*") && (dofldexp(m_TableList[i].ToString().TrimEnd() + "." + xmlSet.TrimEnd()) != "SKIP"))
													    {
														ReportDiffs(m_TableList[i].ToString(), xmlSet, diff.before[k].ToString(), diff.after[k].ToString());
														//Console.WriteLine(" name " + xmlSet);
														//Console.WriteLine(" before" + k + " - " + diff.before[k].ToString() );
														//Console.WriteLine(" after" + k + " - " + diff.after[k].ToString() );
													    }
													}
										    }
										}
										catch (Exception ex)
										{
										    string Errstr = string.Format("FT Compare - Incorrect Delimiter Compare  - " + ex.Message);
										    ReportError(Errstr, 2);
										    if (Show)
											Console.Write("Incorrect Delimiter compare - " + ex.Message + " \n");
										}
										break;
					    //support for templates in here
					    case "TEMPLATE":
								DataTable d1;
								try
								{
								    d1 = m_Templates[TabName];
								    if (Show)
											Console.WriteLine("Template: " + TabName);
								    //Allocate dataset to move data from the databse. It is used to create XML.
								    DataSet OutXmlSrc = new DataSet("OurXml");
								    DataTable dts = new DataTable(TabName);
								    DataRow drs;
								    drs = dts.NewRow();
								    int pos = 0;
								    for (int ii = 0; ii < d1.Columns.Count; ++ii)
								    {
									//Console.Write("\t a1 " + d1.Columns[i].ColumnName);
											dts.Columns.Add(new DataColumn(d1.Columns[ii].ColumnName, Type.GetType("System.String")));
											int len = 0;
											for (int jj = 0; jj < d1.Rows.Count; ++jj)
											{
											    len = Convert.ToInt32(d1.Rows[jj][ii]);
											    drs[ii] = entry.Value.Trim().Substring(pos, len);
											    pos = pos + len;
											    //Console.Write("\t a3 " + d1.Rows[j][i]);
											}
								    }
								    dts.Rows.Add(drs);
								    OutXmlSrc.Tables.Add(dts);
								    srcxml = OutXmlSrc.GetXml();
								    if (Show)
								    {
											Console.WriteLine("Templ compare. srcxml - " + srcxml);
								    }
								}
								catch (Exception ex)
								{
								    string Errstr = string.Format("FT Compare - Incorrect Template  - " + ex.Message);
								    ReportError(Errstr, 2);
								    if (Show)
									Console.Write("Incorrect Template - " + ex.Message + " \n");
								}
		
								try
								{
								    d1 = m_Templates[TabName];
								    //Allocate dataset to move data from the databse. It is used to create XML.
								    DataSet OutXmlTrg = new DataSet("OurXml");
								    DataTable dtt = new DataTable(TabName);
								    DataRow drt;
								    drt = dtt.NewRow();
								    int pos = 0;
								    for (int ii = 0; ii < d1.Columns.Count; ++ii)
								    {
											//Console.Write("\t a1 " + d1.Columns[i].ColumnName);
											dtt.Columns.Add(new DataColumn(d1.Columns[ii].ColumnName, Type.GetType("System.String")));
											int len = 0;
											for (int jj = 0; jj < d1.Rows.Count; ++jj)
											{
											    len = Convert.ToInt32(d1.Rows[jj][ii]);
											    drt[ii] = b[entry.Key].Trim().Substring(pos, len);
											    pos = pos + len;
											    //Console.Write("\t a3 " + d1.Rows[j][i]);
											}
								    }
								    dtt.Rows.Add(drt);
								    OutXmlTrg.Tables.Add(dtt);
								    trgxml = OutXmlTrg.GetXml();
								    if (Show)
								    {
											Console.WriteLine("trgxml - " + trgxml);
								    }
								}
								catch (Exception ex)
								{
								    string Errstr = string.Format("FT Compare - Incorrect Template  - " + ex.Message);
								    ReportError(Errstr, 2);
								    if (Show)
									Console.Write("Incorrect Template - " + ex.Message + " \n");
								}
						//  and do compare like in XML below....
								try
								{
								    StringArrayCompare.StringArrayCompare sCompare = new Simulator.StringArrayCompare.StringArrayCompare(Area);
								    string NdName = "/OurXml/" + TabName;
								    ArrayList results = sCompare.compareMsg(srcxml, trgxml, false, m_Area, NdName);
								    for (int rIdx = 0; rIdx < results.Count; rIdx++)
								    {
											object[] resultsObj = (object[])results[rIdx];
											string xmlSet = (string)resultsObj[0];
											Simulator.StringArrayCompare.diffResult diff = (Simulator.StringArrayCompare.diffResult)resultsObj[1];
											for (int k = 0; k < diff.before.Count; k++)
											{
											    if ((diff.changedFlag[k].ToString() == "*") && (dofldexp(m_TableList[i].ToString().TrimEnd() + "." + xmlSet.TrimEnd()) != "SKIP"))
											    {
														ReportDiffs(m_TableList[i].ToString(), xmlSet, diff.before[k].ToString(), diff.after[k].ToString());
														//Console.WriteLine(" name " + xmlSet);
														//Console.WriteLine(" before" + k + " - " + diff.before[k].ToString() );
														//Console.WriteLine(" after" + k + " - " + diff.after[k].ToString() );
											    }
											}
								    }
								}
								catch (Exception ex)
								{
								    string Errstr = string.Format("FT Compare - Incorrect Template compare  - " + ex.Message);
								    ReportError(Errstr, 2);
								    if (Show)
									Console.Write("Incorrect Template compare - " + ex.Message + " \n");
								}
								break;
					    //support for xml in here. It is different than above. Since the code above only goes 2 nodes deep.
					    case "XML":
								try
								{
									m_SrcXmlArr = new Hashtable();
				
									XmlDocument mDocument = new XmlDocument();
									XmlNode mCurrentNode;
									mDocument.LoadXml(entry.Value.Trim());
									mCurrentNode = mDocument.DocumentElement;
									XmlNodeList nodeList = mCurrentNode.SelectNodes("*");
									foreach (XmlNode node in nodeList)
									{
									    RecurseXmlDocumentNoSiblings(node,0);
									}
									if (Show)
									{
									    foreach (DictionaryEntry ax in m_SrcXmlArr)
										Console.WriteLine("SrcXmlArr list - " + ax.Key + " val - " + ax.Value);
									}
				
									m_TrgXmlArr = new Hashtable();
									mDocument.LoadXml(b[entry.Key].Trim());
									mCurrentNode = mDocument.DocumentElement;
									nodeList = mCurrentNode.SelectNodes("*");
									foreach (XmlNode node in nodeList)
									{
									    RecurseXmlDocumentNoSiblings(node,1);
									}
									if (Show)
									{
									    foreach (DictionaryEntry ax in m_TrgXmlArr)
										Console.WriteLine("TrgXmlArr list - " + ax.Key + " val - " + ax.Value);
									}
									IDictionary<String, String> dsrc = new Dictionary<String, String>();
									foreach (var key in m_SrcXmlArr.Keys)
									{
									 dsrc.Add((String)key, (String)m_SrcXmlArr[key]);
									}
									IDictionary<String, String> dtrg = new Dictionary<String, String>();
									foreach (var key in m_TrgXmlArr.Keys)
									{
									 dtrg.Add((String)key, (String)m_TrgXmlArr[key]);
									}
									CompPairElem (i, dsrc, dtrg, Area);
				
									m_SrcXmlArr = null;
								  m_TrgXmlArr = null;
								  dtrg = null;
								  dsrc = null;
								}
								catch (Exception ex)
								{
								    string Errstr = string.Format("FT Compare - Incorrect XML Format  - " + ex.Message);
								    ReportError(Errstr, 2);
								    if (Show)
									Console.Write("Incorrect XML Format - " + ex.Message + " \n");
								}
								break;
					    case "SKIP":
								if (Show)
								    Console.WriteLine("Skipping compare for " + entry.Key);
								break;
							default:	// Regular expression is given
								if (Show)
								    Console.WriteLine("RegEx for " + entry.Key);
								string res1 = DoExpression(compstr, entry.Value);
								string res2 = DoExpression(compstr, b[entry.Key]);
								if (res1 != res2)
								    ReportDiffs(m_TableList[i].ToString(), entry.Key, res1, res2);
								break;
								}
				    }
					}
        }

        private string DoFieldTranslation(string TblName, string FldName)
        {
            if (m_FldTranslation == null)
                m_FldTranslation = new Dictionary<string, Hashtable>();
            if ((m_FldTranslation != null) && (!(m_FldTranslation.ContainsKey(TblName))))// means we have not tryed to load this table. Load now
            {
                string filename = "c:\\Simulator\\" + m_Area + "\\feed\\" + TblName + "_fldNames.txt";
                Hashtable tbl = null;
                string[] names;
                try { names = System.IO.File.ReadAllLines(filename); }
                catch
                {
                    m_FldTranslation.Add(TblName, tbl);
                    return FldName;
                }
                tbl = new Hashtable();
                int fp = 1;
                string fn;
                foreach (string nm in names)
                {
                    fn = "fld" + fp;
                    tbl.Add(fn, nm);
                    fp++;
                }
                m_FldTranslation.Add(TblName, tbl);
            }
            if ((m_FldTranslation.ContainsKey(TblName)) && (m_FldTranslation[TblName] != null))
            {
                Hashtable tmp = m_FldTranslation[TblName];
                string fld;
                try { fld = tmp[FldName].ToString(); }
                catch { fld = FldName; }
                return fld;
            }
            return FldName;
        }

        private string DoExpression(string compstr, string instr1)
        {
            string res1 = "";
            switch (compstr.Substring(0, 1))
            {
                case "R":
                    // 'ID' is the part of RegEx. 
                    // Example: 			Regex expression = new Regex(@"(?<Id>[0-9]*)\,.*((\r\n)|$)+"); // this is for rules
                    if (Show)
                        Console.WriteLine("Regex - " + compstr.Substring(1));
                    Regex expression = new Regex(@compstr.Substring(1)); // this is for rules
                    var prts1 = expression.Matches(instr1);
                    foreach (Match st in prts1)
                        res1 = res1 + st.Groups["Id"].Value + ",";
                    if (Show)
                        Console.WriteLine("regex res1 - " + res1 + "  string = " + instr1);
                    break;
                default:	// should be our expression
                    res1 = SplitStr(instr1, compstr.Substring(1));
                    if (Show)
                        Console.WriteLine("our regex res1 - " + res1);
                    break;
            }
            return res1;
        }

        private bool FindAlias(string table, string fldname, string srcval, string trgval)
        {
            //Look for source value in the table and compare it with trg value. If = return true.
            // Example of the field.         key = "mif.msg_status.AGED";

            var key = fldname.TrimEnd() + "." + srcval;
            //Console.WriteLine ("tab = " + table + " " + fldname + " " + srcval + " key = " + key);
            List<string> values;
            if (m_FldAlias.TryGetValue(key, out values))
            {
                foreach (string a in values)
                {
                    if (Show)
                        Console.WriteLine("Values for " + key + " src - " + a + " trg - " + trgval);
                    if (trgval.TrimEnd().ToLower() == a.TrimEnd().ToLower())
                        return true;
                }
            }

            return false;
        }

        private string SplitStr(string Buffer, string Expr)
        {
            int pos = 0;
            string res = "";
            string[] pp = Expr.Split(']');
            for (int i = 0; i < pp.Length - 1; i++)
            {
                string[] aa = pp[i].Split('[');
                try
                {
                    if (aa[1] == "=")
                    {
                        res = res + Buffer.Substring(pos, Convert.ToInt32(aa[0]));
                        pos = pos + Convert.ToInt32(aa[0]);
                    }
                    else if (aa[1] == "*")
                    {
                        pos = pos + Convert.ToInt32(aa[0]);
                    }
                    else  //unsupported expression
                    { return Buffer; }
                }
                catch { return Buffer; }
            }
            return res;
        }

        private void ShowTags(string Tbl, string FldName, string Text1, string Text2)
        {
            // 1. Figure out what kind of msgs we have. It could be Swift, psedo Swf, Chp, Fed. The rest we cannot parse.
            string MsgType1 = "";
            string MsgType2 = "";
            int pos = 0, pos1 = 0, pos2 = 0, len = 0;
            string tag_pair1 = "----Original message start----";
            string end_tag_pair1 = "----Original message end----";

            pos1 = Text1.IndexOf(tag_pair1);
            if (pos1 > -1) // found begin of 1st pair
            {
                pos1 += tag_pair1.Length;
                pos2 = Text1.IndexOf(end_tag_pair1);
                if (pos2 > -1) // found the end of 1st pair
                {
                    pos = pos1;
                    len = pos2 - pos1;
                    Text1 = Text1.Substring(pos, len);
                    if (Text1.Substring(0, 2) == "\r\n")
                        Text1 = Text1.Substring(2, len - 2);
                    if (Show)
                        Console.WriteLine("orig - " + Text1);
                }
            }


            pos1 = Text2.IndexOf(tag_pair1);
            if (pos1 > -1) // found begin of 1st pair
            {
                pos1 += tag_pair1.Length;
                pos2 = Text2.IndexOf(end_tag_pair1);
                if (pos2 > -1) // found the end of 1st pair
                {
                    pos = pos1;
                    len = pos2 - pos1;
                    Text2 = Text2.Substring(pos, len);
                    if (Text2.Substring(0, 2) == "\r\n")
                        Text2 = Text2.Substring(2, len - 2);
                    if (Show)
                        Console.WriteLine("orig - " + Text2);
                }
            }


            if ((Text1.IndexOf("{1:") != -1) || (Text1.IndexOf(":20:") != -1))
            {
                MsgType1 = "SWF";  // second msg must be SWF as well
                if ((Text2.IndexOf("{1:") != -1) || (Text2.IndexOf(":20:") != -1))
                    MsgType2 = "SWF";
                else
                {
                    ReportDiffs(Tbl, FldName, Text1, Text2);
                    return;
                }
            }

            if (Text1.IndexOf("[201]") == 2)
            {
                MsgType1 = "CHP";  // second msg must be CHP as well
                if (Text2.IndexOf("[201]") == 2)
                    MsgType2 = "CHP";
                else
                {
                    ReportDiffs(Tbl, FldName, Text1, Text2);
                    return;
                }
            }

            if (Text1.IndexOf("{1500}") != -1)
            {
                MsgType1 = "FED";  // second msg must be FED as well
                if (Text2.IndexOf("{1500}") != -1)
                    MsgType2 = "FED";
                else
                {
                    ReportDiffs(Tbl, FldName, Text1, Text2);
                    return;
                }
            }

            if ((MsgType1 != MsgType2) || (MsgType1 == ""))  // Cannot parse the msg
            {
                ReportDiffs(Tbl, FldName, Text1, Text2);
                return;
            }

            // Now. Start the parsing. Place the results into an Array.

            string Tg1 = "";
            string Tg2 = "";
            if (MsgType1 == "SWF")
            {
                Tg1 = ":";
                Tg2 = ":";
            }
            if (MsgType1 == "CHP")
            {
                Tg1 = "[";
                Tg2 = "]";
            }
            if (MsgType1 == "FED")
            {
                Tg1 = "{";
                Tg2 = "}";
            }

            IDictionary<String, String> TagSrc = new Dictionary<String, String>();

            if (!(SplitString(TagSrc, Tg1, Tg2, Text1)))
            {
                ReportDiffs(Tbl, FldName, Text1, Text2);
                return;
            }

            IDictionary<String, String> TagTrg = new Dictionary<String, String>();

            if (!(SplitString(TagTrg, Tg1, Tg2, Text2)))
            {
                ReportDiffs(Tbl, FldName, Text1, Text2);
                return;
            }
            // Filled up the Tag maps. Now report them	

            foreach (KeyValuePair<String, String> entry in TagSrc)
            {
                if (!TagTrg.ContainsKey(entry.Key)) // Target does not have a key  
                {
                    ReportDiffs(Tbl, entry.Key, entry.Value, "Field is missing");
                }
                else
                {
                    string cm = "=";
                    string fl = Tbl.Trim() + "." + entry.Key;
                    //Console.WriteLine("CHECKING  SWFMIDS Field Expression for  " + fl );
                    try { cm = m_FldExprs[fl].ToString(); }
                    catch { cm = "="; };
                    switch (cm)
                    {
                        case "=":
                            if (entry.Value.Trim() != TagTrg[entry.Key].Trim())
                            {
                                if (Show)
                                {
                                    string v1 = entry.Value;
                                    string v2 = TagTrg[entry.Key];
                                    Console.WriteLine("Val1 - " + v1 + " v1len - " + v1.Length + " v2- " + v2 + " v2len- " + v2.Length);
                                }
                                ReportDiffs(Tbl, entry.Key, entry.Value, TagTrg[entry.Key]);
                            }
                            break;
                        case "SKIP":
                            if (Show)
                                Console.WriteLine("Skipping compare for swfmids." + entry.Key);
                            break;
                        default:	// Regular expression is given
                            string res1 = DoExpression(cm, entry.Value);
                            string res2 = DoExpression(cm, TagTrg[entry.Key]);
                            if (Show)
                                Console.WriteLine("RegEx for swfmids." + entry.Key);
                            if (res1 != res2)
                                ReportDiffs(Tbl.Trim(), entry.Key, res1, res2);
                            break;
                    }
                }
            }
            return;
        }

        private bool SplitString(IDictionary<String, String> TagMap, string Tg1, string Tg2, string Text)
        {
            string Header = "";
            if (Tg1 == ":") //Swift
            {
		if (Text.IndexOf("\r\n:") == -1)  // for some reason the text is missing cr. could be bug in ms sql
			Text = Text.Replace("\n:","\r\n:");
//for (int i=0; i<90; ++i)
//	Console.WriteLine (" i = " + i + Text[i] + "  -  " + ((int)Text[i]).ToString());

                if (Text.IndexOf("{4:") != -1)  // real swift
                {
                    int pos1 = Text.IndexOf("{4:");
                    int pos2 = Text.IndexOf("-}");
                    Header = Text.Substring(Text.IndexOf("{1:"), pos1 + 3 - Text.IndexOf("{1:"));
                    if (pos2 != -1)
                        Text = Text.Substring(pos1 + 3, pos2 - pos1 - 3);
                    else
                        Text = Text.Substring(pos1 + 3);
                }
                else
                {
                    int pos1 = Text.IndexOf(":");
                    int pos2 = Text.IndexOf("000");
                    if (pos2 == -1)
                        pos2 = 0;
                    Header = Text.Substring(pos2, pos1 - pos2);
                    Text = Text.Substring(pos1);
                }
                Text = Text.Substring(Text.IndexOf(':') + 1);
                if (Show1)
                    Console.WriteLine("Header - " + Header + " Rest of text - " + Text);
                TagMap["Header"] = Header;

                string[] swlines = Regex.Split(Text, "\r\n:");
                try
                {
                    foreach (string line in swlines)
                    {
                        string[] swtmps = line.Split(Tg1.ToCharArray(), 2);
                        if (Show1)
                            Console.WriteLine("Tag - " + swtmps[0].ToString() + " val - " + swtmps[1].ToString());
                        TagMap[swtmps[0].ToString()] = swtmps[1].ToString();
                    }
                }
                catch (Exception ex)
                {
                    string Errstr = string.Format("Fatal Swift Parsing error - " + Text + " Error: " + ex.Message);
                    ReportError(Errstr, 2);
                    if (Show1)
                        Console.WriteLine("Fatal Swift Parsing error - " + Text);
                    return false;
                }
                return true;
            }

            if (Tg1 == "[")
                Text = Text.Substring(Text.IndexOf('[') + 1);
            if (Tg1 == "{")
                Text = Text.Substring(Text.IndexOf('{') + 1);

            string[] lines = Text.Split(Tg1.ToCharArray());
            try
            {
                foreach (string line in lines)
                {
                    if (Show1)
                        Console.WriteLine("Line - " + line.ToString());
                    string[] Ln = line.Split(Tg2.ToCharArray());
                    if (Show1)
                        Console.WriteLine("Tag - " + Ln[0] + " Txt - " + Ln[1]);
                    TagMap[Ln[0]] = Ln[1];
                }
            }
            catch (Exception ex)
            {
                string Errstr = string.Format("Fatal Parsing error - " + Text + " Error: " + ex.Message);
                ReportError(Errstr, 2);
                if (Show1)
                    Console.WriteLine("Fatal Parsing error - " + Text);
                return false;
            }

            return true;
        }

        private void ReportDiffs(string Tbl, string FldName, string Val1, string Val2)
        {
            m_Totals[Tbl] = (int)m_Totals[Tbl] + 1;	//Update Totals
            m_UpdNo++;
            string[] Fname = FldName.Split('.');
            string fn;
            try { fn = Fname[1].ToString(); }
            catch { fn = Fname[0]; };
            if (Fname[0].ToString() == "messagefreetext")
            {
                try { fn = Fname[2].ToString(); }
                catch { fn = Fname[0]; };
            }
//      Console.WriteLine("table- "+Tbl+ " fld "+FldName);
            if (Fname.Length > 3)  // usually XML fld
            	fn = Fname[Fname.Length-2]+"."+Fname[Fname.Length-1];

            if (Tbl.StartsWith("message_external_interaction"))
            	fn =Fname[Fname.Length-1];

            fn = fn.Replace("'", "''");
            Val1 = Val1.Replace("'", "''");
            Val2 = Val2.Replace("'", "''");
            if (Val1.Length > 1024)
                Val1 = Val1.Substring(0, 1024);
            if (Val2.Length > 1024)
                Val2 = Val2.Substring(0, 1024);
            if (fn.Length > 32)	// strange code. I found ':' inside of the field text body. It throws off the parsing.
                fn = fn.Substring(0, 32);

            m_DiffUpd = m_DiffUpd + string.Format("insert into DiffResults values ({0},'{1}','{2}','{3}','{4}','{5}','{6}');", m_Batch, m_Mid1, m_Mid2, fn, Val1, Val2, Tbl);
            if ((UpdDb) && (m_UpdNo > 5))
            {
                m_Connection.Execute(m_DiffUpd, true);
                m_DiffUpd = "";
                m_UpdNo = 0;
            }
            if ((Show) || (ShowRes))
                Console.WriteLine("Batch - " + m_Batch + "  mid1 - " + m_Mid1 + " " + FldName + " Src value - " + Val1 + " not = " + Val2);
        }
        
	static void RecurseXmlDocumentNoSiblings(XmlNode root, int Arr)
    	{
		string hldName = m_FldName;
		if (root is XmlElement)
		{
		    Console.WriteLine("from no sibling - "+root.Name);
		    if (root.HasChildNodes)
		    {
			m_FldName = root.Name;
			RecurseXmlDocument(root.FirstChild,Arr);
		    }
		}
		else if (root is XmlText)
		{
		    string text = ((XmlText)root).Value;
		    Console.WriteLine("xmltext1"+text);
		}
		else if (root is XmlComment)
		{
		    string text = root.Value;
		    Console.WriteLine("xmlcomment1"+text);
		    if (root.HasChildNodes)
		    {
			m_FldName = root.Name;
			RecurseXmlDocument(root.FirstChild, Arr);
			m_FldName=hldName;
		    }
		}
		m_FldName = hldName;
	}
        static void RecurseXmlDocument(XmlNode root, int Arr)
        {
		string hldName=m_FldName;
		if (root is XmlElement)
		{
		    if (root.HasChildNodes)
		    {
			m_FldName = m_FldName+"."+root.Name;
		   // Console.WriteLine("haschildroot.name- "+FldName+"  hold name- " + hldName);
			RecurseXmlDocument(root.FirstChild,Arr);
			m_FldName=hldName;
		    }
		    if (root.NextSibling != null)
		    {
	//            Console.WriteLine("!=nexsiblingroot.name- "+FldName+"  hold name- " + hldName);
			RecurseXmlDocument(root.NextSibling,Arr);
			m_FldName=hldName;
		    }
		}
		else if (root is XmlText)
		{
		    string text = ((XmlText)root).Value;
		    if (Arr == 0)
		    {
		    	    for (int i = 1; i < 7; i++)
		    	    {
		    	    	if ( m_SrcXmlArr.ContainsKey(m_FldName) )
		    	    		m_FldName = m_FldName + i.ToString();
		    	    	else
		    	    		break;
		    	    }
			    m_SrcXmlArr.Add(m_FldName, text);
//		    Console.WriteLine("1 add to srcxmlarr. - "+m_FldName+" value."+text+">");
		    }
		    else
		    {
		    	    for (int i = 1; i < 7; i++)
		    	    {
		    	    	if ( m_TrgXmlArr.ContainsKey(m_FldName) )
		    	    		m_FldName = m_FldName + i.ToString();
		    	    	else
		    	    		break;
		    	    }
			    m_TrgXmlArr.Add(m_FldName, text);
	//	    Console.WriteLine("1 add to trgxmlarr. - "+m_FldName+" value."+text+">");
		    }

		  //  Console.WriteLine("1. - "+m_FldName+" value."+text+">");
		}
		else if (root is XmlComment)
		{
		    string text = root.Value;
	  //          Console.WriteLine("xmlcomment - "+ FldName +" value."+text+">");
		    if (root.HasChildNodes)
		    {
			m_FldName = m_FldName+"."+root.Name;
		   // Console.WriteLine("haschildroot.name- "+FldName+"  hold name- " + hldName);
			RecurseXmlDocument(root.FirstChild,Arr);
			m_FldName=hldName;
		    }
		    if (root.NextSibling != null)
		    {
	//            Console.WriteLine("!=nexsiblingroot.name- "+FldName+"  hold name- " + hldName);
			RecurseXmlDocument(root.NextSibling,Arr);
			m_FldName=hldName;
		    }
		}
		m_FldName = hldName;
	}

        private void LoadTemplates()
        {
            //Init templates...
            if ((m_NoTemplate) || ((m_Templates != null) && (m_Templates.Count > 0))) // this is the list of templates for various handoff records.
                return;

            m_Templates = new Dictionary<string, DataTable>();
            DataSet ds = new DataSet();

            string DocName = "c:\\Simulator\\" + m_Area + "\\feed\\template.xml";
            try
            {
                ds.ReadXml(DocName);
            }
            catch (Exception ex)
            {
                if (ex.Message.IndexOf("Could not find file") != 1) // see if we have a template file
                    m_NoTemplate = true;
                else
                {
                    string Errstr = string.Format("FT Compare LoadTemplates - " + ex.Message);
                    ReportError(Errstr, 1);
                }
                return;
            }
            m_NoTemplate = false;
            foreach (DataTable table in ds.Tables)
            {
                m_Templates.Add(table.TableName, table);
                if (Show)
                {
                    Console.WriteLine("Loading Templates for templ name - " + table.TableName);
                    for (int i = 0; i < table.Columns.Count; ++i)
                    {
                        Console.Write("\t name " + table.Columns[i].ColumnName);
                        for (int j = 0; j < table.Rows.Count; ++j)
                            Console.Write("\t length " + table.Rows[j][i]);
                        Console.WriteLine();
                    }
                }
            }
        }

        private void LoadTabSql()
        {
            try
            {
                if (m_TableSql.Count > 0)	// this is the virtual fields with special SQLs for them.
                    return;
                XmlDocument doc = new XmlDocument();
                string DocName = "c:\\Simulator\\" + m_Area + "\\feed\\tablesql.xml";
                doc.Load(DocName);
                XmlNodeList nodes = doc.SelectNodes("/TABLESQL/TBSQL");
                foreach (XmlNode node in nodes)
                {
                    string tbl = node.SelectSingleNode("table").InnerText;
                    string sql = node.SelectSingleNode("sql").InnerText.Replace("&lt;", "<").Replace("&gt;", ">");
                    if (Show)
                        Console.WriteLine("Adding table - " + tbl + " sql - " + sql);
                    m_TableSql.Add(tbl, sql);
                }
            }
            catch (Exception ex)
            {
                string Errstr = string.Format("FT Compare LoadSQL - Key table - TableName - " + ex.Message);
                ReportError(Errstr, 2);
            }
        }

        private void LoadFldAliases()
        {
            if ((m_FldAlias != null) && (m_FldAlias.Count > 0))	// field alias table.
                return;
            try
            {
                m_FldAlias = new Dictionary<string, List<string>>();
                XmlDocument doc = new XmlDocument();
                string DocName = "c:\\Simulator\\" + m_Area + "\\feed\\fldalias.xml";
                doc.Load(DocName);
                XmlNodeList nodes = doc.SelectNodes("/FLDALIAS/ALIASSET");
                foreach (XmlNode node in nodes)
                {
                    string fname = node.SelectSingleNode("fldname").InnerText;
                    string val = node.SelectSingleNode("value").InnerText;
                    string als = node.SelectSingleNode("alias").InnerText;
                    string f1 = fname + "." + val;
                    if (Show)
                        Console.WriteLine("Adding alias - " + f1 + " alias - " + als);
                    var key = f1;
                    List<string> values;
                    if (!m_FldAlias.TryGetValue(key, out values))
                    {
                        values = new List<string>();
                        m_FldAlias.Add(key, values);
                    }
                    values.Add(als);
                }
            }
            catch (Exception ex)
            {
                string Errstr = string.Format("FT Compare LoadFldAliases - FldAliases - " + ex.Message);
                ReportError(Errstr, 2);
            }
        }

        private void LoadFldExprs()
        {
            try
            {
                if (m_FldExprs.Count > 0)	// this is the list of fields form messagefreetext or outbound text.
                    return;
                XmlDocument doc = new XmlDocument();
                string DocName = "c:\\Simulator\\" + m_Area + "\\feed\\fldexprs.xml";
                doc.Load(DocName);
                XmlNodeList nodes = doc.SelectNodes("/FLDTABLE/FLDENTRY");
                foreach (XmlNode node in nodes)
                {
                    string tbl = node.SelectSingleNode("table").InnerText;
                    string fname = node.SelectSingleNode("fname").InnerText;
                    string exp = node.SelectSingleNode("expression").InnerText;
                    string f1 = tbl + "." + fname;
                    if (Show)
                        Console.WriteLine("Adding field - " + f1 + " expression - " + exp);
                    m_FldExprs.Add(f1, exp);
                }
            }
            catch (Exception ex)
            {
                string Errstr = string.Format("FT Compare LoadFldExprs - FldExprs - " + ex.Message);
                ReportError(Errstr, 2);
            }
        }

        private void LoadKeys(string TableName)
        {
            int Pk = 0, Cnt = 0;
            DateTime UpdTime = DateTime.MinValue;
            string Cmd = string.Format("select pk, UpdTime from localkeyname where tabname='{0}'", TableName);
            try
            {
                if (m_ReadConnection.OpenDataReader(Cmd))
                {
                    while (m_ReadConnection.SQLDR.Read())
                    {
                        Pk = m_ReadConnection.SQLDR.GetInt32(0);
                        UpdTime = m_ReadConnection.SQLDR.GetDateTime(1);
                    }
                    m_ReadConnection.CloseDataReader();
                    if (UpdTime == null)
                        UpdTime = DateTime.Now;
                    /* EXAMPLE of usage
                    int compareResult = firstDate.CompareTo(secondDate);
                    if (compareResult < 0)
                        Console.WriteLine("First date is earlier");
                    else if (compareResult == 0)
                        Console.WriteLine("Both dates are same");
                    else
                        Console.WriteLine("First date is later");
                    */
                    if (Show)
                        Console.WriteLine(" time cmp - " + " " + UpdTime.CompareTo(m_lastUpd) + UpdTime.ToString("O") + " " + m_lastUpd.ToString("O") + " " + m_KeyTabLoaded + " " + TableName);
                    if ((UpdTime.CompareTo(m_lastUpd) < 0) &&
                         (m_KeyTabLoaded.Length != 0) && (TableName == m_KeyTabLoaded))
                    {
                        if (Show1)
                            Console.WriteLine("Key table does not need to be updated");
                        return;
                    }
                }
                if (ShowTime)
                    Console.WriteLine("   Loading Keys starts at " + DateTime.Now.ToString("HH:mm:ss.ffff"));
                Cmd = string.Format("select count(*) from LocalKeys where TabNameInd={0}", Pk);
                if (m_ReadConnection.OpenDataReader(Cmd))
                {
                    while (m_ReadConnection.SQLDR.Read())
                    {
                        Cnt = m_ReadConnection.SQLDR.GetInt32(0);
                    }
                    m_ReadConnection.CloseDataReader();
                }
            }
            catch (Exception ex)
            {
                string Errstr = string.Format("FT Compare LoadKeys - Key table - TableName - " + ex.Message);
                ReportError(Errstr, 2);
            }
            if (Show1)
                Console.WriteLine("Reload Key table");
            m_KeyTabLoaded = TableName;
            m_lastUpd = DateTime.Now;
            try { m_Keys.Clear(); }
            catch { };  // first time it does not exist
            m_Keys = new Hashtable(Cnt);
            m_TableSql = new Hashtable();
            m_FldExprs = new Hashtable();

            try { m_TableList.Clear(); }
            catch { };
            m_TableList = new ArrayList();
            // Create a list of tables to deal with.
            Cmd = string.Format("select distinct XmlSet from LocalKeys where TabNameInd='{0}' and Include='Y'", Pk);
            if (m_ReadConnection.OpenDataReader(Cmd))
            {
                while (m_ReadConnection.SQLDR.Read())
                {
                    m_TableList.Add(m_ReadConnection.SQLDR[0].ToString().Trim().ToLower());
                }
                m_ReadConnection.CloseDataReader();
            }
            if (Show)
            {
                foreach (string a in m_TableList)
                    Console.WriteLine("Table list - " + a);
            }
            // Create keytable for this area.
            Cmd = string.Format("select CmpKey, XmlSet, CompExpression, Include from LocalKeys where TabNameInd='{0}'", Pk);
            if (m_ReadConnection.OpenDataReader(Cmd))
            {
                while (m_ReadConnection.SQLDR.Read())
                {
                    string CmpKey = m_ReadConnection.SQLDR[0].ToString().Trim().ToLower();
                    string XmlSet = m_ReadConnection.SQLDR[1].ToString().Trim().ToLower();
                    string CompExp = m_ReadConnection.SQLDR[2].ToString().Trim();
                    string Inc = m_ReadConnection.SQLDR[3].ToString().Trim();
                    if (Inc == "Y")
                    {
                        string a = XmlSet + "." + CmpKey;
                        m_Keys.Add(a, CompExp);
                    }
                }
                m_ReadConnection.CloseDataReader();
            }
            if (Show)
            {
                foreach (DictionaryEntry a in m_Keys)
                    Console.WriteLine("Key list - " + a.Key + " val - " + a.Value);
            }
            if (ShowTime)
                Console.WriteLine("   Loading Keys ends at " + DateTime.Now.ToString("HH:mm:ss.ffff"));
            return;
        }

        private void ReportError(string ErrMsg, int Arg)
        {
            if (Show)
                Console.WriteLine(ErrMsg);
            _simLog.Source = "FtFeeder";

            // Write an error entry to the event log.
            if (Arg == 2)
                _simLog.WriteEntry(ErrMsg, EventLogEntryType.Error);
            SimLog.log.write(m_Area, ErrMsg, true);
            //		    m_Connection.Connect(true,m_Area);
            // for later		    string Cmd = string.Format("update BatchDescr set BatchStatus = 'Aborted. Check Logs.' where BatchId = '{0}'", Pkey);
            // for later		    m_Connection.Execute(Cmd,true);
            return;
        }

        private static List<JournalDiffs> DiffList(List<JournalEntry> Src, List<JournalEntry> Trg)
        {
            /*
            The algorithm to come up with diffs.
            Iterate thru Src and Trg
            1. Check if Src = Trg
               if yes advance both Src and Trg to the next entry
               if no look for Src value in Trg 
                  if found report ALL skipped entries as diff ( fname=, srcvalue='field missing', trgvalue=those skipped entries ) sync the LISTs
                  if not found set src list to the prev index and look for Trg in Src 
                    if found report ALL skipped entries as diff ( fname=, trgvalue='field missing', srcvalue=those skipped entries ) sync the LISTs
                    else set trg list to the prev index and report those entries as diffs.
               advance Src and Trg to the next entry
            */

            List<JournalDiffs> AllDiff = new List<JournalDiffs>();
            List<JournalDiffs> TmpDiff = new List<JournalDiffs>();
            int SrcTot = Src.Count - 1;
            int TrgTot = Trg.Count - 1;
            int SrcCnt = 0;
            int TrgCnt = 0;
            for (; ; )	// loop until we process ALL entries from both lists
            {
                if ((SrcCnt > SrcTot) && (TrgCnt > TrgTot)) // we checked ALL entries in BOTH lists.
                    break;
                if (SrcCnt > SrcTot)	//Src is exhasted. Report all the rest of TRG as diffs
                {
                    string[] s1 = Trg[TrgCnt].Fname.Split('.');
                    AllDiff.Add(new JournalDiffs() { Fname = s1[1], SrcValue = "Field missing", TrgValue = Trg[TrgCnt].Value });
                    TrgCnt++;
                    continue;
                }
                if (TrgCnt > TrgTot)	//Trg is exhasted. Report all the rest of SRC as diffs
                {
                    string[] s1 = Src[SrcCnt].Fname.Split('.');
                    AllDiff.Add(new JournalDiffs() { Fname = s1[1], SrcValue = Src[SrcCnt].Value, TrgValue = "Field missing" });
                    SrcCnt++;
                    continue;
                }
                if (Src[SrcCnt].Value == Trg[TrgCnt].Value)
                {
                    SrcCnt++;
                    TrgCnt++;
                    TmpDiff.Clear();
                    continue;
                }
                else
                {
                    int TrgSave = TrgCnt;
                    bool trgFound = false;
                    while (true)
                    {
                        if (TrgCnt > TrgTot)
                            break;
                        /*
                            Console.WriteLine("Srccnt - " + SrcCnt + " Trgcnt - " + TrgCnt + " Trgtot - " + TrgTot+" Srctot - " + SrcTot);
                            Console.WriteLine("Srcval - " + Src[SrcCnt].Value);
                            Console.WriteLine("Trgval - " + Trg[TrgCnt].Value );
                        */
                        if (Src[SrcCnt].Value == Trg[TrgCnt].Value)
                        {
                            trgFound = true;
                            break;
                        }
                        else
                        {
                            string[] s1 = Trg[TrgCnt].Fname.Split('.');
                            TmpDiff.Add(new JournalDiffs() { Fname = s1[1], SrcValue = "Field missing", TrgValue = Trg[TrgCnt].Value });
                        }
                        TrgCnt++;
                    }
                    if (trgFound) // Add all entries from TmpDiff to AllDiff.
                    {
                        for (int i = 0; i < TmpDiff.Count; i++)
                            AllDiff.Add(new JournalDiffs() { Fname = TmpDiff[i].Fname, SrcValue = TmpDiff[i].SrcValue, TrgValue = TmpDiff[i].TrgValue });
                        TmpDiff.Clear();
                    }
                    else
                    {
                        TrgCnt = TrgSave;
                        int SrcSave = SrcCnt;
                        bool srcFound = false;
                        TmpDiff.Clear();
                        while (true)	//Go thru Src list . Try to sync it.
                        {
                            if (SrcCnt > SrcTot)
                                break;
                            if (Src[SrcCnt].Value == Trg[TrgCnt].Value)
                            {
                                srcFound = true;
                                break;
                            }
                            else
                            {
                                string[] s1 = Src[SrcCnt].Fname.Split('.');
                                TmpDiff.Add(new JournalDiffs() { Fname = s1[1], TrgValue = "Field missing", SrcValue = Src[SrcCnt].Value });
                            }
                            SrcCnt++;
                        }
                        if (srcFound)
                        {
                            Console.WriteLine("report missings trg = " + Trg[TrgCnt].Value + " SRC = " + Src[SrcCnt].Value);
                            for (int i = 0; i < TmpDiff.Count; i++)
                                AllDiff.Add(new JournalDiffs() { Fname = TmpDiff[i].Fname, SrcValue = TmpDiff[i].SrcValue, TrgValue = TmpDiff[i].TrgValue });
                            TmpDiff.Clear();
                        }
                        else	//Could not find src as well. Real Diffs. Report.
                        {
                            SrcCnt = SrcSave;
                            string[] s1 = Src[SrcCnt].Fname.Split('.');
                            AllDiff.Add(new JournalDiffs() { Fname = s1[1], SrcValue = Src[SrcCnt].Value, TrgValue = Trg[TrgCnt].Value });
                            SrcCnt++;
                            TrgCnt++;
                            TmpDiff.Clear();
                        }
                    }
                }
            }
            //			Console.WriteLine(Src.Count+"---"+Src[0].Fname+ " - "+Src[2].Value);
            return AllDiff;
        }
    }
}
