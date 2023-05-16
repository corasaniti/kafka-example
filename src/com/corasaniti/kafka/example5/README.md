# Single Mode Consumer (different group)

	In questo esempio si vogliono creare due Consumer distinti iscritti
	allo stesso topic (demo2) ognuno dei quali riceve  TUTTI i messaggi 
	che il Producer invia al topic.
		
	Per fare questo i due Consumer sono iscritti allo  stesso topic con
	lo stesso group.id.
	
	Ogni consumatore effettua il polling dei messaggi  inviati al topic 
	e li stampa a schermo. In questo modo, tutti i consumatori ricevono 
	tutti i messaggi inviati dal produttore al topic. 
		
	Si fa notare il parametro auto.offset.reset con il valore  earliest 
	è una configurazione utilizzata dai consumatori  Kafka per definire 
	cosa succede quando un consumatore si unisce ad un gruppo di 
	consumatori o ha un offset di consumo non valido.
	Quando il parametro auto.offset.reset è impostato su "earliest" ciò 
	significa che il consumatore inizierà a leggere i  messaggi dal più 
	vecchio offset  disponibile nel topic, anche  se il  consumatore si 
	unisce al gruppo per la prima volta o se ha un offset non valido.
	
	In pratica, questo significa che il consumatore inizierà a leggere i 
	messaggi  dal primo messaggio disponibile nel topic,  consentendo di 
	ricevere tutti i messaggi presenti nel topic, inclusi quelli inviati 
	prima che il consumatore si unisse al gruppo.
	
	È importante notare che se il parametro auto.offset.reset è impostato 
	su earliest e non ci sono messaggi storici nel topic,  il consumatore 
	inizierà comunque a leggere i nuovi messaggi man mano che arrivano al 
	topic.

	

