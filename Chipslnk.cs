/*                                                             
 Copyright (c) 1999 - 2005 by EFT Technologies, Inc.
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
CHP --> MTS
msg type 31
ISN -> seq number from the other bank. 10 was received by chips for us with the assigned ISN. It is mapped into
       the ISN of 31 that we receive.
SSN -> from the other bank. Same as above.
OSN -> unique per bank (not line). and is assigned to 31s by CHIPS. We use NewOsnNumber stored proc to do it.

MTS --> CHP
msg type 10
We receive 10s from MTS and reply with 25. 25 contains the SSN (NewSsnNumber), ISN (NewIsnNumber) 

On MTS side.
They look at CHIPSOSN log to determine gaps in OSNs
             CHIPSPRN log to determine gaps in Payment Resolver Number (PRN) It comes from [38] resolver
             We use NewRsnNumber.
 * 
 * 28-Nov-15    JR  Synch code between Jacob and John
*/
using System;
using Simulator.DBLibrary;
using Simulator.Interfaces;
using Simulator.MQClass;
using Simulator.QueueInterface;

namespace Chipslnk
{
	class Chipslnk
	{
		private DBAccess m_Connection;

		private string m_Area;
		private string m_LineName;
		private string m_QueueName;
		private string m_RcvQueueName;
		private string m_ProcName;
		private string m_SessionId;
		private string m_ChpAba;
		private DateTime m_lastActivity;
		private DateTime m_TimeoutTime;
		private DateTime m_LastWrite;
		private SynchInterface m_transportConnection;		// interface to mq classes

		int m_CurrentSequence;// Current link sequence
		int m_RsnSeqNo;// Resolver sequence #
		int m_NumberofQueueItemstoCache;
		QueueInterface m_QI;

		// MQ specific stuff

		string m_inputQueueName;
		string m_outputQueueName;
		string m_queueMgrName;
		string m_channel;
		
		int m_ReadTimeout;
		bool m_Logged = false;
		bool m_Newsess = true;
		bool m_InactFlag = false;
		int m_Noof85 = 0;
		int m_Noof31 = 0;
		int m_Noof10 = 0;
		int Max10 = 10;
		int Max31 = 50;
		int MaxTimeOut = 3;
		bool Show = true;

		[STAThread]
		static void Main(string[] args)
		{
			Chipslnk cl=new Chipslnk();
			cl.Run(args);
		}

		private void Run(string[] args)
		{
			if(args.Length < 2)
			{
				Console.Write("Correct syntax for invoking this executable is Area LineName\r\n");
				return;
			}

			m_QI = null;
			m_Area = args[0];
			m_LineName = args[1];			// as in CHIPSnn
			string sh = "";
			try
			{
				sh = args[2];
				if (sh=="Y")
					Show = true;
			}
			catch
			{
				Show = false;
			}
			m_Connection=new DBAccess();
			m_Connection.Connect(true,m_Area);
			m_NumberofQueueItemstoCache=10;
			m_QueueName="LNK_"+m_LineName;
			m_RcvQueueName = "RCV_"+m_LineName;
			m_ProcName="Chips"+"_"+m_Area+"_"+m_LineName; 
			GetLineContext();
			MsgLoop();
	}


	private void MsgLoop()
	{
		if (Show)
			Console.WriteLine ("Setting m_readTimeout - 3");

		m_ReadTimeout=MaxTimeOut; // Set initial timeout for the first login. Next one will be indefinite.
		for(;;)
		{
			try
			{
// Init all variables.
				m_Newsess = true;
				m_Logged = false;
				m_InactFlag = false;
				m_Noof85 = 0;
				m_Noof31 = 0;
				m_Noof10 = 0;
				m_LastWrite=DateTime.Now;
				m_transportConnection = new SimMQ(m_Area, m_queueMgrName, m_channel);
				Console.WriteLine("Connecting");
				if( m_transportConnection.Connect(m_inputQueueName,m_outputQueueName))
					Link_process();
				else
				{
					Console.WriteLine(string.Format("Line {0} cannot connect to mq address {0} and {1}",m_inputQueueName,m_outputQueueName));
					break;
				}
			}
			catch (Exception ex)
			{
				Console.Write(string.Format("Problem connecting to MQ for line {0},Error {1}",m_LineName,ex));
				break;
			}
			m_ReadTimeout = 0; // next read sits forever until the other side activates
			m_transportConnection.DisConnect();

			// Delete this line from the ChpLineList.
			if (Show)
				Console.WriteLine("Delete from ChpLineList for line " + m_LineName);
			string Cmd=string.Format("delete from ChpLineList where LineName='{0}' ", m_LineName);
			m_Connection.Execute(Cmd,true);
// We could be here IF the other side is disconnected. Since there is a time delay between the other side
// line down and 'till we react (no answer on 58) we might have msgs on outbound link for the othe side.
// We need to see if the other lines are up. If they are try to move msgs from this MQ to other line.
// The indicator is a line in the CHPLINELIST.
			Cmd=string.Format("select COUNT(*) from LNK_{0}", m_LineName);
			m_Connection.OpenDataReader(Cmd);
			m_Connection.SQLDR.Read();
			int MsgNo=m_Connection.SQLDR.GetInt32(0);
			m_Connection.CloseDataReader();
			if (MsgNo > 0)
			{
			        Cmd=string.Format("select min(msgcount) as cnt, linename from chplinelist group by msgcount, linename");
				m_Connection.OpenDataReader(Cmd);
				string line1=m_LineName;
				try 
				{
					m_Connection.SQLDR.Read();
					line1 = "LNK_" + m_Connection.SQLDR["LineName"].ToString().TrimEnd();
					if (Show)
						Console.Write ("Moving msgs from " + m_LineName + " to " + line1 + "\n");
					if(m_QI == null)
						m_QI = new QueueInterface("LNK_"+m_LineName,m_NumberofQueueItemstoCache,m_Area);
					bool Done=false;
					for(;;)
					{
						for(int index=0;index < m_NumberofQueueItemstoCache;index++)
						{
							QueueItem qi= m_QI.getQueueItem();
							if(qi==null)
							{
								Done = true;
								break;
							}
							Cmd=string.Format("insert into LNK_{0} (prio,msgcount,qbltext) values ('{1}', {2}, '{3}')", line1,qi.Priority,qi.Msgcount,qi.Text);
							m_Connection.Execute(Cmd,false);
							m_Connection.Execute(string.Format("delete from LNK_{0} where qbl={1} and prio='{2}'",m_LineName,qi.Qblid,qi.Priority),true);
						}
						if(Done)
							break;
					}
				}
				catch
				{
				}
				m_Connection.CloseDataReader();
			}
		}
	}
	
	private void Link_process()
	{
		string readBuffer = "";
		for (;;)   //loop until the other side is inactive.
		{
			try
			{
				if(m_transportConnection.Read(ref readBuffer,m_ReadTimeout))	//
				{
					if (Show)
						Console.WriteLine ("After read");
					m_ReadTimeout=MaxTimeOut;
					m_InactFlag = false;
					if (m_Logged)
					{
						if (m_Newsess)
						{
							m_Newsess = false;
							m_SessionId=string.Format("{0:yyyyMMddhhmmss}",DateTime.Now);
							string Cmd=string.Format("insert into ChpStats (SessionNo,StartTime,EndTime) values ('{0}','{1:yyyy-MM-dd hh:mm:ss}','{2:yyyy-MM-dd hh:mm:ss}')",m_SessionId,DateTime.Now,DateTime.Now);
							m_Connection.Execute(Cmd,true);
						}
						int msgTypeNdx=readBuffer.IndexOf("QT-",0,readBuffer.Length);	// see what kind of msg
						if(msgTypeNdx != -1)
						{
							msgTypeNdx+="QT-".Length;
							if(readBuffer.Substring(msgTypeNdx,2).Equals("10"))	// a msg
							{    
								string Cmd = string.Format("exec NewSsnNumber");
								m_Connection.Execute(string.Format("update linkcontrol set NextSeqNo={0} where LineName='{1}'",m_CurrentSequence,m_LineName),false);
								Cmd=string.Format("insert into {0} (qbltext) values ('{1}')", m_RcvQueueName,readBuffer.Replace("'", "''") );
								m_Connection.Execute(Cmd,true);
								if (Show)
									Console.WriteLine ("Got payment # {0:0000}",m_Noof10);
								m_Noof10 ++;
								if (m_Noof10 > Max10) // Start sending
								{
									if (Show)
										Console.WriteLine(string.Format("Rcvd {0} msgs. Start Sending 31s",m_Noof10) ); 
									if (!Send_msgs ()) // write fails. disconnect 
										return;
								}
							}
							else // not payment
								ProcessNonPayment(readBuffer);
							continue;
						}
						else
							m_Connection.RecordEvent(1,m_ProcName,string.Format("{0}:Bad message format {1}", DateTime.Now,readBuffer), m_Area);
					}
					else  // Not Logged
					{
						int msgTypeNdx=readBuffer.IndexOf("QT-",0,readBuffer.Length);	// see what kind of msg
						if(msgTypeNdx != -1)
						{
							if (Show)
								Console.WriteLine ("Not Logged, - msg {0}",readBuffer.Substring(msgTypeNdx,2));
							msgTypeNdx+="QT-".Length;
							m_Logged = true;
// We are logging in. The ChpLineList should not contain this line. so, insert it.
							string Cmd=string.Format("select * from ChpLineList where LineName = '{0}'", m_LineName);
							m_Connection.OpenDataReader(Cmd);
							bool hasRows = m_Connection.SQLDR.HasRows;
					                m_Connection.CloseDataReader();
					                if (hasRows)
					                {
								Cmd=string.Format("update ChpLineList set msgcount=0 where LineName = '{0}'", m_LineName);
								m_Connection.Execute(Cmd,true);
					                }
							else
							{
								Cmd=string.Format("insert into ChpLineList (LineName,MsgCount) values ('{0}',0)", m_LineName);
								m_Connection.Execute(Cmd,true);
							}
							if(readBuffer.Substring(msgTypeNdx,2).Equals("05"))	// an ack. Discard all 05s
								continue;
							else  // non acks ENQ to the RCVq
							{
								Cmd=string.Format("insert into {0} (qbltext) values ('{1}')", m_RcvQueueName,readBuffer.Replace("'", "''") );
								m_Connection.Execute(Cmd,true);
								if (!Send_msgs ()) // write fails. disconnect 
									return;
							}
						}
					}
				}
				else  // Empty MQ read
				{
					if (m_InactFlag)
					{
						if (Show)
							Console.WriteLine ("Incativ - true, No data from read");
						DateTime currentTime=DateTime.Now;
						TimeSpan spannedTime=currentTime.Subtract(m_TimeoutTime);
						TimeSpan writeTime=currentTime.Subtract(m_LastWrite);
			// try to send after 3 sec of inactivity in case our reformatter is slow or 31s are appear. 
						if ((writeTime.TotalSeconds >= 3 ) && (m_Logged))
						{
							if (Show)
								Console.WriteLine ("Delay write");
							if (!Send_msgs ()) // write fails. disconnect 
								return;
						}
						if(spannedTime.TotalSeconds >= 60 )
						{ // Send 85 , add 1 to noof85, change m_timeouttime
							m_TimeoutTime=DateTime.Now;
							m_Noof85++;
							string qblText=string.Format("CM-{0}0000279T20060127153935PASSWORD123!@#$%-MCQT-85[085]02{0:000000}{0:000000}{0:000000}{0:000000}{0:000000}-TQ",m_ChpAba,m_CurrentSequence,m_RsnSeqNo,m_CurrentSequence,m_CurrentSequence,m_CurrentSequence);
							if(!m_transportConnection.Write(qblText))			// if write fails, flush transaction
							{
								m_Connection.RecordEvent(1,m_ProcName,string.Format("{0}:MQ write logon error in Chipslink {1}", DateTime.Now,m_LineName), m_Area);
								return;	// and reconnect
							}
							if (m_Noof85 > 1)
								return;  // disconnect and try to start new session
						}
						else  // has not timedout yet.
							continue;
					}
					else // inactflag = false means that we were active 
					{
						if (Show)
							Console.WriteLine ("Incativ - false, No data from read");
						m_InactFlag = true;
						m_lastActivity=DateTime.Now;
						m_TimeoutTime=DateTime.Now;
						m_Connection.Execute(string.Format("update ChpStats set EndTime='{0:yyyy-MM-dd hh:mm:ss}' where SessionNo='{1}'",DateTime.Now,m_SessionId),true);
						if (m_Logged)
							if (!Send_msgs ()) // write fails. disconnect 
								return;
						else
							continue;
					}
				}
			}
			catch (Exception e)
			{
				m_Connection.RecordEvent(1,m_ProcName,string.Format("{0}:Read error in link {1} - {2}", DateTime.Now,m_LineName, e.Message), m_Area );
				return;
			}
		}
	}
	
	private void ProcessNonPayment (string buffer)
	{
		int msgTypeNdx=buffer.IndexOf("QT-",0,buffer.Length);	// see what kind of msg
		if(msgTypeNdx != -1)
		{
			msgTypeNdx+="QT-".Length;
			if (Show)
				Console.WriteLine ("Process non payment {0}",buffer.Substring(msgTypeNdx,2));
			if(buffer.Substring(msgTypeNdx,2).Equals("05"))	// an ack. Discard all 05s except for 31s
			{
				if (buffer.Substring(msgTypeNdx+9,2).Equals("85"))
					m_Noof85 = 0;
				if (buffer.Substring(msgTypeNdx+9,2).Equals("31"))
				{	// find the QBL from CM- header and change Ack31q Acked to Y.
					msgTypeNdx=buffer.IndexOf("CM-",0,buffer.Length);
					int qbl = Convert.ToInt32(buffer.Substring(msgTypeNdx+7,7));
					try
					{
						m_Connection.Execute(string.Format("update Ack31q set Acked='Y' where qbl={0}",qbl),true);
					}
					catch 
					{
						m_Connection.RecordEvent(1,m_ProcName,string.Format("{0}:error updating orig 31. Qbl-{2:0000000} {1}", DateTime.Now,m_LineName,qbl), m_Area);
					} 
					if (Show)
						Console.WriteLine ("Got 05 for previously sent 31. Noof31 - " + m_Noof31);
					m_Noof31--;
				}
				return;
			}
			else  // non acks ENQ to the RCVq
			{
				string Cmd=string.Format("insert into {0} (qbltext) values ('{1}')", m_RcvQueueName,buffer.Replace("'", "''") );
				m_Connection.Execute(Cmd,true);
			}
		}
	}

	private bool Send_msgs()
	{
		m_Noof10 = 0;
		m_lastActivity=DateTime.Now;
		m_LastWrite=DateTime.Now;
		if(m_QI == null)
			m_QI = new QueueInterface("LNK_"+m_LineName,m_NumberofQueueItemstoCache,m_Area);
		string qblText;
		int qblId=0;
		int qblPriority=0;
		if (Show)
			Console.WriteLine ("Send routine. Step in");
		for(;;)
		{
			for(int index=0;index < m_NumberofQueueItemstoCache;index++)
			{
				QueueItem qi= m_QI.getQueueItem();
				if(qi==null)
				{
					if (m_Logged)	// back to receiving
						return true;
					else
					{
						if (Show)
							Console.WriteLine ("Send routine. non logged in.");
						System.Threading.Thread.Sleep(2000);
						break;
					}
				}
				qblText = qi.Text;
				qblId = qi.Qblid;
				m_Logged = true;
				qblPriority= qi.Priority;
				qblText = qblText.Replace("???????", string.Format("{0:0000000}",qblId));
				qblText = qblText.Replace("=======",string.Format("{0:0000000}",m_CurrentSequence));
				if(!m_transportConnection.Write(qblText))			// if write fails, flush transaction
				{
					m_Connection.RecordEvent(1,m_ProcName,string.Format("{0}:MQ write error in Chipslink {1}", DateTime.Now,m_LineName), m_Area);
					return false;			// and reconnect
				}
				m_lastActivity=DateTime.Now;

				if (Show)
					Console.WriteLine (string.Format("Seq# {0}. Sending - {1}",m_CurrentSequence, qblText));
				
				string Cmd = string.Format("exec NewOutNumber '{0}'", m_LineName); // update our internal line seq number.
				if(m_Connection.OpenDataReader(Cmd))
				{
					m_Connection.SQLDR.Read();
					m_CurrentSequence=m_Connection.SQLDR.GetInt32(0);
					m_Connection.CloseDataReader();
				}
				//m_Connection.Execute(string.Format("update linkcontrol set OutSeqNo={0} where LineName='{1}'",m_CurrentSequence,m_LineName),false);
				m_Connection.Execute(string.Format("update ChpStats set EndTime='{0:yyyy-MM-dd hh:mm:ss}' where SessionNo='{1}'",DateTime.Now,m_SessionId),false);
				m_Connection.Execute(string.Format("delete from {0} where qbl={1} and prio='{2}'",m_QueueName,qblId,qblPriority),true);
				//m_CurrentSequence++;
				int msgTypeNdx=qblText.IndexOf("QT-",0,qblText.Length);	// see what kind of msg
				if(msgTypeNdx != -1)
				{
					msgTypeNdx+="QT-".Length;
					if(qblText.Substring(msgTypeNdx,2).Equals("31"))	// payment
					{	// Add orig to Ack31q to retain all sent msgs with SSN ans OSN and Ack status
						int Ssn = Convert.ToInt32(qblText.Substring(msgTypeNdx+30,7));
						int Osn = Convert.ToInt32(qblText.Substring(msgTypeNdx+43,6));
						Cmd=string.Format("insert into Ack31q (Qbl,Osn,Ssn,Acked,qblText) values ({0},{1},{2},'Y','{3}')", qblId,Osn,Ssn,qblText.Replace("'", "''") );
						m_Connection.Execute(Cmd,true);
						m_Noof31++;
						if (m_Noof31 > Max31)
						{
							if (Show)
								Console.WriteLine(string.Format("Sent {0} msgs.  Go to Recv mode,",m_Noof31) ); 
							return true;
						}
					}
				}
			}
		}		
	}

#region commonstuff

	private void GetLineContext()
	{
		string Cmd=string.Format("select NextSeqNo, inputMQName,outputMQName,MQMgrName,ConnChannel,RsnSeqNo from LinkControl where linename= '{0}'",m_LineName);
		m_Connection.OpenDataReader(Cmd);
		m_Connection.SQLDR.Read();
		int colNdx=0;
		m_CurrentSequence=m_Connection.SQLDR.GetInt32(colNdx++);
		m_inputQueueName=m_Connection.SQLDR[colNdx++].ToString().TrimEnd().ToUpper();
		m_outputQueueName=m_Connection.SQLDR[colNdx++].ToString().TrimEnd().ToUpper();
		m_queueMgrName=m_Connection.SQLDR[colNdx++].ToString().TrimEnd().ToUpper();
		m_channel=m_Connection.SQLDR[colNdx++].ToString().TrimEnd().ToUpper();
		m_RsnSeqNo=m_Connection.SQLDR.GetInt32(colNdx++);
		m_Connection.CloseDataReader();

//   ... and get chpABA
            	Cmd = string.Format("select ChpAba from SimulatorControl");
		m_Connection.OpenDataReader(Cmd);
            	m_Connection.SQLDR.Read();
            	m_ChpAba = m_Connection.SQLDR["ChpAba"].ToString().TrimEnd();
		m_Connection.CloseDataReader();
	}

#endregion
	}
}
