package crawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
//import java.sql.ResultSet;
import java.sql.SQLException;

public class SearchData {

	static int entry[]= new int[15];
	private String time;
	private String name;
	private String message;
	private String modified_message;
	private String link;
	private String title_id;
	private String title;
	private String thread_id;
	private static String type="title";
	final String DB_URL = "jdbc:mysql://bo.favista.in/favista";
	Connection conn = null;
	private String USER="intern_mukul";
	private String PASS="skw124DJ";
	private int queryTimeout=10;
	int numbers=0;
	String entity_id[]=new String[1000];
	String entity_name[]=new String[1000];
	String entity[]=new String[1000];
	
	public void search()
	{
		Crawler crawler=new Crawler();
		String line=null;
		String text=null;
		int counter=0;
		try
		{
		int i,j;
		File textfile=new File("sample.txt");
		FileReader r1 = new FileReader(textfile);
		BufferedReader read=new BufferedReader(r1);
		while((text=read.readLine())!=null)
		{
			System.out.println("the line from sample is "+text);
			String values[]=text.split(" ");
		//String file_content[]=values[1].split(" ");
		
		for(i=0;i<values.length;i++)
		{
			crawler.hashfunction1(values[i]);
			crawler.hashfunction2(values[i]);
			crawler.hash_filename="hashfile"+crawler.file_value+".txt";
			File file=new File(crawler.hash_filename);
			FileReader r =new FileReader(file);
			BufferedReader reader =new BufferedReader(r);
			while((line=reader.readLine())!=null)
			{
				counter++;
				String content[]=line.split(":");
				String file_content[]=content[1].split(" ");
				for(j=0;j<file_content.length;j++)
				if(file_content[j].equals(values[i]))
				{
					System.out.println("found the value at index "+counter+" and file no is "+crawler.file_value);
					System.out.println("content is");
					System.out.println(content[0]+":"+content[1]+":"+content[2]+":"+content[3]);
					//entity_id[numbers]=content[2];
					//entity_name[numbers]=content[1];
					//entity[numbers]=content[3];
					//numbers++;
					break;
				}
			}
			counter=0;
			reader.close();
		}
		}
		read.close();
		}
		catch(IOException ex)
		{
			System.out.println("problem in searching");
			ex.printStackTrace();
		}
		/*try
		{
		int length,j;
		String words[]=modified_message.split(" ");
		for(length=0;length<=words.length;length++)
		{
			crawler.hashfunction1(words[length]);
			crawler.hashfunction2(words[length]);
			crawler.hash_filename="hashfile"+crawler.file_value+".txt";
			File file=new File(crawler.hash_filename);
			FileReader r =new FileReader(file);
			BufferedReader reader =new BufferedReader(r);
			while((line=reader.readLine())!=null)
			{
				counter++;
				String content[]=line.split(":");
				String file_content[]=content[1].split(" ");
				for(j=0;j<file_content.length;j++)
				if(file_content[j].equals(words[length]))
				{
					System.out.println("found the value at index "+counter+" and file no is "+crawler.file_value);
					System.out.println("content is");
					System.out.println(content[0]+":"+content[1]+":"+content[2]+":"+content[3]);
					//entity_id[numbers]=content[2];
					//entity_name[numbers]=content[1];
					//entity[numbers]=content[3];
					//numbers++;
					break;
				}
			}
			counter=0;
			reader.close();
			
		}
		}
		catch(IOException ex)
		{
			System.out.println("error in matching");
			ex.printStackTrace();
		}*/
		
	}
	
	public void check()
	{
		try
		{
		int fileno;
		String filename;
		for(fileno=0;fileno<=15;fileno++)
		{
			filename="hashfile"+fileno+".txt";
			File file =new File(filename);
			FileReader re=new FileReader(file);
			BufferedReader reader=new BufferedReader(re);
			String line;
			while((line=reader.readLine())!=null)
			{
				String content[]=line.split(":");
				if(!content[1].equals("null"))
				{
					entry[fileno]++;
				}
			}
			System.out.println("entries in fileno "+fileno+"  is "+entry[fileno]);
			reader.close();
			}
		}
		catch(IOException ex)
		{
			System.out.println("error in checking");
			ex.printStackTrace();
		}
	}
	
	public void getWebData()
	{
		int counter=0,i;
		try
		{
		File file=new File("sample.txt");
		FileReader r=new FileReader(file);
		BufferedReader reader=new BufferedReader(r);
		String line;
		while((line=reader.readLine())!=null)
		{
			counter++;
			String content[]=line.split(":");
			if(counter==1&&content[1].equals("post"))
			{
				type="post";
			}
			else if(content[0].equals("title"))
			{
				title=content[1];
			}
				if(content[0].equals("time"))
				{
					time=content[1];
				}
				else if(content[0].equals("name"))
				{
					name=content[1];
				}
				else if(content[0].equals("title_id"))
				{
					title_id=content[1];
				}
				
				else if(content[0].equals("thread_id"))
				{
					thread_id=content[1];
				}
			
				else if(content[0].equals("message")&&!content[0].equals("modified_message")&&!content[0].equals("link"))
				{
					message+=content[1];
				}
				else if(content[0].equals("modified_message")&&!content[0].equals("link"))
				{
					modified_message+=content[1];
				}
				else if(content[0].equals("link"))
				{
					for(i=1;i<content.length;i++)
					{
						link+=content[i];
					}
				}
				
				else
				{
					System.out.println("please check the getWebData part syntax");
				}
		
		}
		reader.close();
		}
		catch(IOException ex)
		{
			System.out.println("error in copying webdata to variables");
			ex.printStackTrace();
		}
	}
	
	public void insert_title()
	{
		String sql;
		try
		{
			sql="insert into title values('"+title+"','"+thread_id+"','"+name+"','"+message+"','"+time+"','"+link+"');";
			System.out.println("sql is "+sql);
			PreparedStatement selQuery=conn.prepareStatement(sql);
			selQuery.setQueryTimeout(queryTimeout);
			selQuery.executeUpdate();
			selQuery.close();
		}
		catch(SQLException sq)
		{
			System.out.println("problem in inserting in title table");
			sq.printStackTrace();
		}
	}
	
	public void insert_post()
	{
		String sql;
		try
		{
			sql="insert into post values('"+title_id+"','"+name+"','"+message+"','"+time+"','"+link+"');";
			System.out.println("sql is "+sql);
			PreparedStatement selQuery=conn.prepareStatement(sql);
			selQuery.setQueryTimeout(queryTimeout);
			selQuery.executeUpdate();
			selQuery.close();
		}
		catch(SQLException sq)
		{
			System.out.println("problem in inserting in title table");
			sq.printStackTrace();
		}
	}
	
	public void insert_project()
	{
		String sql;
		int i;
		try
		{
			for(i=1;i<numbers;i++)
			{
				if(entity[i].equals("project_name"))
					entity[i]="";
				else if(entity[i].equals("locality_name"))
				{
					entity[i]="";
				}
				else if(entity[i].equals("builder_name"))
				{
					entity[i]="";
				}
				else if(entity[i].equals("city_name"))
				{
					entity[i]="";
				}
				else if(entity[i].equals("city_name"))
				{
					entity[i]="";
				}
			sql="insert into  tagged_entities values('"+title_id+"','"+entity_id[i]+"','"+entity_name[i]+"',null,'"+entity[i]+"');";
			System.out.println("sql is "+sql);
			PreparedStatement selQuery=conn.prepareStatement(sql);
			selQuery.setQueryTimeout(queryTimeout);
			selQuery.executeUpdate();
			selQuery.close();
			}
		}
		catch(SQLException sq)
		{
			System.out.println("problem in inserting in title table");
			sq.printStackTrace();
		}
	}
	
	public void establishConn()
	{
		// Register JDBC driver
				try{
					Class.forName("com.mysql.jdbc.Driver");
				}catch (Exception c) {
					System.out.println("\nO teri !!! error aa gyi : " + c);
					//sendmail("error : " + c);
				}


				//Open a connection
				System.out.println("\nConnecting to database...");

				try{
					conn = DriverManager.getConnection(DB_URL,USER,PASS);
				}catch (java.sql.SQLException s){
					//sendmail("error : " + s);
					System.out.println(("O teri !!! favista database se connect karne mein error aa gyi : " + s));}

	}
	
	public static void main(String[] args) {
		//int i,output=0;
		SearchData s=new SearchData();
		//s.establishConn();
		//s.getWebData();
		//s.search();
		Crawler cr=new Crawler();
		/*cr.hashfunction1("Palm Springs");
		cr.hashfunction2("Palm Springs");
		cr.hashfunction1("Park Springs");
		cr.hashfunction2("Park Springs");
		cr.hashfunction1("Palm Street");
		cr.hashfunction2("Palm Street");*/
		
		cr.hashfunction1("avenue");
		cr.hashfunction2("avenue");
		cr.hashfunction1("avenu");
		cr.hashfunction2("avenu");
		
		/*if(type.equals("post"))
		{
			s.insert_post();
		}
		else 
			s.insert_title();
		s.insert_project();*/
		
		/*s.check();
		for(i=0;i<=100;i++)
			output+=entry[i];
		System.out.println("total entry in all files is "+output);*/

	}

}
