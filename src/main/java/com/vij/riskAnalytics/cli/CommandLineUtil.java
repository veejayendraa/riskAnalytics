package com.vij.riskAnalytics.cli;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CommandLineUtil {

	private static final Logger log = Logger.getLogger(CommandLine.class.getName());
	private Options options = new Options();
	private String[] args = null;
	private LocalDate businessDate = null;
	private String hierarchy = null;
	private String portfolio = null;
	
	public CommandLineUtil(String args[])
	{
		options.addOption("b", "business date", true, "business date in yyyy-MM-dd");
		options.addOption("p", "portfolio", true, "portfolio name");
		options.addOption("h", "hierarhcy", true, "hierarchy");
		this.args = args;
		parse();

	}
	
	private void parse() 
	{
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;
		try {
			 cmd = parser.parse(options, args);
			 if (cmd.hasOption("b")){
				 log.log(Level.INFO,"Business date is : "+ cmd.getOptionValue("b"));
				this.businessDate = LocalDate.parse(cmd.getOptionValue("b"), DateTimeFormatter.ofPattern("yyyy-MM-dd",Locale.ENGLISH)); 
			 }else{
				 log.log(Level.SEVERE, "Missing b option");
				 help();
			 }
			 if (cmd.hasOption("p") || cmd.hasOption("h")){
				 if(cmd.hasOption("p"))
				 {
					 log.log(Level.INFO,"Portfolio is : "+ cmd.getOptionValue("p"));
					 this.portfolio = cmd.getOptionValue("p");
				 }
				 
				 if(cmd.hasOption("h"))
				 {
					 log.log(Level.INFO,"Hierarchy is : "+ cmd.getOptionValue("h"));
					 this.hierarchy = cmd.getOptionValue("h");
				 }
			 }else{
				 log.log(Level.SEVERE, "Missing both p and h option. Giving one out of p or h option is mandatory ");
				 help();
			 }
		}catch (ParseException e) 
		{
			   log.log(Level.SEVERE, "Failed to parse comand line properties", e);
			   help();
		 }
	}
	
	private void help() {
		  // This prints out some help
		  HelpFormatter formater = new HelpFormatter();
		  formater.printHelp("Main", options);
				  System.exit(0);
		 }
	public LocalDate getBusinessDate(){
		return this.businessDate;
	}
	
	public String getPortfolio()
	{
		return this.portfolio;
	}

	public String getHierarchy() {
		return hierarchy;
	}
}
