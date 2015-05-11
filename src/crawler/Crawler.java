package crawler;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
//import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
//import java.io.PrintWriter;
//import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
//import java.util.Scanner;

public class Crawler {
	int fileno;
	String filename;
	int max=15;
	int index;
	int max_records=10000;
	final String DB_URL = "jdbc:mysql://bo.favista.in/favista";
	Connection conn = null;
	private String USER="intern_mukul";
	private String PASS="skw124DJ";
	private int queryTimeout=10;
	private String builder_name;
	private int power;
	int file_value=0;
	private double x=39;
	private int index_value;
	String hash_filename;
	private int builder_id;
	private String project_name;
	private int project_id;
	private String city_name;
	private int city_id;
	private String cluster_name;
	private int cluster_id;
	private String locality_name;
	private int locality_id;
	static int entry;
	
	
	public void files()
	{
		System.out.println("files are made");
		try
		{
			for(fileno=0;fileno<=max;fileno++)
			{
				filename="hashfile"+fileno+".txt";
			FileWriter wr =new FileWriter(filename,true);
			BufferedWriter writer=new BufferedWriter(wr);
			for(index =1;index<=max_records;index++)
			{
				writer.write(index+":null"+":idnull:type");
			    writer.newLine();
			}
			writer.close();
			}
			System.out.println("initializing file content");
		}
		catch(IOException ex)
		{
			System.out.println("problem in creating or writing to file");
			ex.printStackTrace();
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
	
	public void getDetails_builders()
	{
		System.out.println("fetching details from database");
		String sql;
		try
		{
			sql="select distinct name,id from fv_builders where name<>'null' and name <> '';";
			System.out.println("sql is "+sql);
			PreparedStatement selQuery=conn.prepareStatement(sql);
			selQuery.setQueryTimeout(queryTimeout);
			ResultSet rs = selQuery.executeQuery();
			while(rs.next())
			{
				builder_name=rs.getString("name").trim();
				builder_id=rs.getInt("id");
				hashfunction1(builder_name);
				hashfunction2(builder_name);
				inserting_values_files(builder_name,builder_id,"builder_name");
				entry++;
				System.out.println("builders name is "+builder_name);
			}
			rs.close();
		}
		catch(SQLException sq)
		{
			System.out.println("error in inserting builder information");
			sq.printStackTrace();
		}
		
	}
	
	
	public void getDetails_project()
	{
		System.out.println("fetching project details from database");
		String sql;
		try
		{
			sql="select distinct name,id from fv_projects where name <> 'null' and name <>'';";
			System.out.println("sql is "+sql);
			PreparedStatement selQuery=conn.prepareStatement(sql);
			selQuery.setQueryTimeout(queryTimeout);
			ResultSet rs = selQuery.executeQuery();
			while(rs.next())
			{
				project_name=rs.getString("name").trim();
				project_id=rs.getInt("id");
				hashfunction1(project_name);
				hashfunction2(project_name);
				inserting_values_files(project_name,project_id,"project_name");
				entry++;
				System.out.println("project name is "+project_name);
			}
			rs.close();
		}
		catch(SQLException sq)
		{
			System.out.println("error in inserting project information");
			sq.printStackTrace();
		}
		
	}
	
	public void getDetails_cities()
	{
		System.out.println("fetching cities details from database");
		String sql;
		try
		{
			sql="select distinct name,id from fv_cities where name<>'null' and name<>'';";
			System.out.println("sql is "+sql);
			PreparedStatement selQuery=conn.prepareStatement(sql);
			selQuery.setQueryTimeout(queryTimeout);
			ResultSet rs = selQuery.executeQuery();
			while(rs.next())
			{
				city_name=rs.getString("name").trim();
				city_id=rs.getInt("id");
				hashfunction1(city_name);
				hashfunction2(city_name);
				inserting_values_files(city_name,city_id,"city_name");
				entry++;
				System.out.println("city name is "+city_name);
			}
			rs.close();
		}
		catch(SQLException sq)
		{
			System.out.println("error in inserting cities information");
			sq.printStackTrace();
		}
	}

	public void getDetails_clusters()
	{
		System.out.println("fetching  clusters details from database");
		String sql;
		try
		{
			sql="select distinct name,id from fv_clusters where name<>'null' and name<>'';";
			System.out.println("sql is "+sql);
			PreparedStatement selQuery=conn.prepareStatement(sql);
			selQuery.setQueryTimeout(queryTimeout);
			ResultSet rs = selQuery.executeQuery();
			while(rs.next())
			{
				cluster_name=rs.getString("name").trim();
				cluster_id=rs.getInt("id");
				hashfunction1(cluster_name);
				hashfunction2(cluster_name);
				inserting_values_files(cluster_name,cluster_id,"cluster_name");
				entry++;
				System.out.println("cluster name is "+cluster_name);
			}
			rs.close();
		}
		catch(SQLException sq)
		{
			System.out.println("error in inserting clusters information");
			sq.printStackTrace();
		}
	}
	
	public void getDetails_locations()
	{
		System.out.println("fetching locality details from database");
		String sql;
		try
		{
			sql="select distinct name,id from fv_locations where name<>'' and name<>'null';";
			System.out.println("sql is "+sql);
			PreparedStatement selQuery=conn.prepareStatement(sql);
			selQuery.setQueryTimeout(queryTimeout);
			ResultSet rs = selQuery.executeQuery();
			while(rs.next())
			{
				locality_name=rs.getString("name").trim();
				locality_id=rs.getInt("id");
				hashfunction1(locality_name);
				hashfunction2(locality_name);
				inserting_values_files(locality_name,locality_id,"locality_name");
				entry++;
				System.out.println("location name is "+locality_name);
			}
			rs.close();
		}
		catch(SQLException sq)
		{
			System.out.println("error in inserting locality information");
			sq.printStackTrace();
		}
	}
	
	public void hashfunction1(String value)
	{
		int i;
		file_value=0;
		power=value.length()-1;
		for(i=0;i<=power;i++)
		{
			if(i==0)
			file_value=file_value+((int)(value.charAt(i))-65)*(int)Math.pow(x,power-i);
			else 
				file_value=file_value+((int)(value.charAt(i))-97)*(int)Math.pow(x,power-i);
		}
		
		file_value=Math.abs(file_value)%max;
		System.out.println("value of hash function1 is "+file_value);
	}
	
	public void hashfunction2(String value)
	{
		int i;
		index_value=0;
		power=value.length()-1;
		for(i=0;i<=power;i++)
		{
			if(i==0)
			index_value=index_value+((int)(value.charAt(i))-65)*(int)Math.pow(x,power-i);
			else
				index_value=index_value+((int)(value.charAt(i))-97)*(int)Math.pow(x,power-i);
		}
		index_value=Math.abs(index_value)%max_records;
		System.out.println("value of hash value 2 is "+index_value);
		
	}
	
	public void inserting_values_files(String field1,int field2,String type)
	{
		//int i=2;
		//String text="";
		int counter=2;
		try
		{
			System.out.println("write in a file "+file_value);
			hash_filename="hashfile"+file_value+".txt";
			 // Open the file that is the first
            // command line parameter
            FileInputStream fstream = new FileInputStream(hash_filename);
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
            String strLine=null;
            StringBuilder fileContent = new StringBuilder();
            //Read File Line By Line
            while ((strLine = br.readLine()) != null) {
                // Print the content on the console
                //System.out.println(strLine);
                String tokens[] = strLine.split(":");
                if (tokens.length > 0) {
                    // Here tokens[0] will have value of ID
                    if (Integer.parseInt(tokens[0])>=index_value&&tokens[1].equals("null")&&counter>0) {
                        tokens[1] = field1;
                        tokens[2]= Integer.toString(field2);
                        tokens[3]=type;
                        String newLine = tokens[0] + ":" + tokens[1]+":"+tokens[2]+":"+tokens[3];
                        fileContent.append(newLine);
                        fileContent.append("\n");
                        counter=-1;
                    } else {
                        // update content as it is
                        fileContent.append(strLine);
                        fileContent.append("\n");
                    }
                }
            }
            // Now fileContent will have updated content , which you can override into file
            FileWriter fstreamWrite = new FileWriter(hash_filename);
            BufferedWriter out = new BufferedWriter(fstreamWrite);
            out.write(fileContent.toString());
            out.close();
            //Close the input stream
            fstream.close();
			
			
		}
		catch(IOException e)
		{
			System.out.println("problem in writing to file");
			
			e.printStackTrace();
		}
	}
	

	public void searchitem()
	{
		int counter=0;
		try
		{
		int i;
		String values[]={"TATA Housing","Bestech","Anantraj"};
		String line=null;
		for(i=0;i<values.length;i++)
		{
			hashfunction1(values[i]);
			hashfunction2(values[i]);
			hash_filename="hashfile"+file_value+".txt";
			File file=new File(hash_filename);
			FileReader r =new FileReader(file);
			BufferedReader reader =new BufferedReader(r);
			while((line=reader.readLine())!=null)
			{
				counter++;
				String content[]=line.split(":");
				if(content[1].equals(values[i]))
				{
					System.out.println("found the value at index "+counter+" and file no is "+file_value);
					break;
				}
			}
			counter=0;
			reader.close();
		}
		}
		catch(IOException ex)
		{
			System.out.println("problem in searching");
			ex.printStackTrace();
			
			
		}
	}
	
	public void closeConnection()
	{
		try
		{
		conn.close();
		}
		catch(Exception e)
		{
			System.out.println("error in clossing connection");
			e.printStackTrace();
		}
	}
	
	public static void main(String args[])
	{
		Crawler first=new Crawler();
		first.files();
		
		
		first.establishConn();
		first.getDetails_builders();
		first.closeConnection();
		
		
		first.establishConn();
		first.getDetails_project();
		first.closeConnection();
		
		
		first.establishConn();
		first.getDetails_cities();
		first.closeConnection();
		
		
		first.establishConn();
		first.getDetails_clusters();
		first.closeConnection();
		
		
		first.establishConn();
		first.getDetails_locations();
		first.closeConnection();
		
		
		System.out.println("data is inserted into files");
		System.out.println("total entry in files are "+entry);
		//first.searchitem();
	}

}



/*
File file=new File(hash_filename);
FileReader r=new FileReader(file);
BufferedReader reader =new BufferedReader(r);
File file1=new File("temp.txt");
FileWriter wr1=new FileWriter(file1);
BufferedWriter writer1=new BufferedWriter(wr1);
String line=null;
while((line=reader.readLine())!=null)
{
	System.out.println("the line is "+line);
	String file_content[]=line.split(":");
	counter=Integer.parseInt(file_content[0]);
	if(counter>=index_value&&file_content[1].equals("null")&&i>0)
	{
		line=line.replaceFirst(index_value+":null",""+index_value+":"+builder_name);
		i=-1;
	}
	writer1.write(line);
	writer1.newLine();
	//text=text+line+"";
}
reader.close();
writer1.close();

File file3=new File(hash_filename);
FileWriter wr2=new FileWriter(file3);
PrintWriter pr=new PrintWriter(wr2);
pr.println("");
pr.close();


File file2 =new File(hash_filename);
FileWriter wr=new FileWriter(file2);
BufferedWriter writer=new BufferedWriter(wr);
writer.write(text);
writer.close();
File file4=new File("temp.txt");
FileReader r4=new FileReader(file4);
BufferedReader reader4 =new BufferedReader(r4);
File file5=new File(hash_filename);
FileWriter wr5=new FileWriter(file5,true);
BufferedWriter writer5=new BufferedWriter(wr5);
String line1=null;
while((line1=reader4.readLine())!=null)
{
	writer5.write(line1);
	writer5.newLine();
}
reader4.close();
writer5.close();
System.out.println("file is written\n\n");*/



/*int i=2;
String line;
int counter;
String text="";
System.out.println("data from database is to write in file "+file_value);
try
{
	hash_filename="C:\\Users\\mukul\\workspace\\crawler\\"+"hashfile"+file_value+".txt";
	File file=new File(hash_filename);
	//File file1=new File("temp.txt");
	//Scanner scan=new Scanner(file).useDelimiter("\n");
	FileReader scan=new FileReader(file);
	BufferedReader reader =new BufferedReader(scan);
	FileWriter wr=new FileWriter(file,true);
	//BufferedWriter writer=new BufferedWriter(wr);
	System.out.println(reader.readLine());
	while((line=reader.readLine())!=null)
	{
		String file_content[]=line.split(":");
		counter=Integer.parseInt(file_content[0]);
		if(counter>=index_value&&file_content[1].equals("null")&&i>0)
		{
			index_value=counter;
			System.out.println("value of counter is "+counter);
			line=line.replaceFirst(index_value+":null",""+index_value+":"+builder_name);
			i=-1;
		}
		System.out.println("the line is "+line);
		text=text+ line+"                    ";
		writer.write(line);
		writer.newLine();
	}
	System.out.println("the text to insert is "+text+"\n\n\n");
	wr.write(text);
	wr.close();
	reader.close();*/
	
	/* FileChannel inputChannel = null;
	 
	 FileChannel outputChannel = null;
	 
	     try {
	 
	         inputChannel = new FileInputStream("temp.txt").getChannel();
	
	         outputChannel = new FileOutputStream(hash_filename).getChannel();
	 
	         outputChannel.transferFrom(inputChannel, 0, inputChannel.size());
	 
	     } 
	     finally {
	 
	         inputChannel.close();
	 
	         outputChannel.close();
	 
	     }*/

	
	/*FileReader r=new FileReader(hash_filename);
	BufferedReader read=new BufferedReader(r);
	PrintWriter write=new PrintWriter(hash_filename);
	while((line=read.readLine())!=null)
	{
		writer.flush();
	}
	read.close();
	write.close();
	
	FileReader r1=new FileReader("tmp.txt");
	BufferedReader read1=new BufferedReader(r1);
	FileWriter wr1=new FileWriter(hash_filename,true);
	BufferedWriter writer1=new BufferedWriter(wr1);
	while((line=read1.readLine())!=null)
	{
		writer1.write(line);
		writer1.newLine();
	}
	writer1.close();
	read1.close();*/
	//FileWriter writer=new FileWriter(file);
	//line=text.replaceFirst(index_value+":null",""+index_value+":"+builder_name);
	//writer.write(line);
	//writer.close();
		//String file_content[]=line.split(":");
		/*//counter=Integer.parseInt(file_content[0]);
		if(counter>=index_value && file_content[1].equals("null"))
		{
			//text=file_content[0]+":"+builder_name;
			FileWriter writer=new FileWriter(file,true);
			line=line.replace("null",builder_name);
			writer.write(line);
			writer.close();
			break;
		}*/
	/*counter=0;
		FileReader r=new FileReader(hash_filename);
		BufferedReader read=new BufferedReader(r);
		PrintWriter write=new PrintWriter(hash_filename);
		while((line=read.readLine())!=null)
		{
			counter++;
			writer.flush();
			if(counter>100)
				break;
		}
		read.close();
		write.close();*/
		
	
/*	}
catch(IOException ex)
{
	System.out.println("problem in writing to hashfile "+file_value);
	ex.printStackTrace();
}*/
