package com.corasaniti.kafka.common.message;

import java.util.Date;
import java.util.UUID;
import java.text.SimpleDateFormat;

//Classe di esempio per l'oggetto personalizzato da inviare come messaggio
public class MessageTime {
 
		public String id;
		public String message;
		public String timestamp;
		public String dateTime;

		//Necessario per la deserializzazione JSON
		public MessageTime() {		     
		 }

		public MessageTime(String message) {
			this.id = UUID.randomUUID().toString(); 
			this.message = message;
			this.timestamp = String.valueOf((new Date().getTime()));	
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			Date date = new Date();  
			this.timestamp = String.valueOf((date.getTime()));	
			this.dateTime  = sdf.format(date);  
		}
		
}
