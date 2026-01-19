# Domande pre impostazione del lavoro
1. Come individuate i dati già processati?
    Mi approccerei alla situazione dando una chiave di valore unica che possa essere usata per confrontare i dati presenti nel db e quelli che stanno venendo caricati nel CSV, i principali indiziati sarebbero incident_id con date_id. Le mie alternative sono sul valutare la frequenza di ingestione dei dati ricercando quindi un valore prefissato all'interno del csv oppure una controllo totale che vada a sempre a vedere dal db quali siano i dati mancanti
2. Come gestite l’arrivo  dinuovi dati senza duplicazioni?
    L’arrivo di nuovi dati viene gestito evitando duplicazioni attraverso un controllo basato su una combinazione di chiave univoca dell’evento e timestamp di ingestione. Il timestamp consente di identificare i dati più recenti, mentre la chiave logica permette di verificare se un evento è già stato caricato. In questo modo è possibile gestire correttamente ingestione incrementale e prevenire duplicati anche in presenza di ricaricamenti o ritardi nella disponibilità dei dati, ad esempio le date possono ripetersi con più incidenti nello stesso giorno ma l'identificiativo della chiamata rimane sempre come chiave unica per ogni singolo record.
3. Come scrivete sui vari layer? Perché?
    La modalità di scrittura che andrei a prediligere sarebbe append. Riuscendo a fare un controllo prima dell'ingestione dei dati stessi vorrei quindi evitare di creare un processo troppo lungo. Avendo una robustezza di pipeline che fa controlli interni sulla integrità del dato la scrittura in aggiunta senza andare a riscrivere tutta l'intera tabella mi sembra la cosa più corretta
4. Come garantite la coerenza dei KPI nel tempo?
    Ingerendo sempre dati nuovi, le domande di business poste incentrate su dimensioni temporali e modalità di avvenimento non vengono intaccate da quella che diventerebbe una continua ingestione di nuovi dati, anzi andranno sempre di più ad arricchire i valori che abbiamo di fronte in modo da poterli consultare per ulteriori miglioramenti del servizio chiesto. Non stiamo andando ad ampliare gli attributi del dato stiamo soltanto numericamente aggiornandoli quindi una KPI temporale rimane comunque consultabile ed utile anche se cambia di valore col tempo che passa.
5. Cosa succede se una run schedulata fallisce a metà pipeline?
    Una run schedulata fallita lascia un commento sul tipo di errore avvenuto e cancella tutto il lavoro svolto fino a quel momento. Possibile tramite la cancellazione delle tabelle temporanee che vengono create durante la pipeline stessa
6. Quali assunzioni avete fatto sui dati in ingresso?
    i dati in ingresso sono dati pronti all'uso, ma non puliti. Completamente paragonabili a quelli ricevuti dal dataset, vengono registrati giorno per giorno ed alla fine della giornata aggiornano il db
7. Dovete modificare il modello dati con l'integrazione della Fase 2? Perchè?
    Il modello non dovrebbe venir modificato visto che i dati in arrivo non sono strutturalmente differenti ma mantengono la stessa teoria dei precedenti
Consiglio: fare un nuovo branch per la fase 2
Per rispondere a queste domande è vietato usare assistenti AI, loro risponderebbero in 10
secondi, voi dovete ragionarci ed abituarvi a sbagliare.
8. In cosa la vostra pipeline della Fase 2 differisce concettualmente da quella della Fase 1?
    la prima pipeline esiste per essere una sorta di trial run, comincia e finisce ed è un prodotto statico. Il lavoro attuale vuole portare ad un continuo uso del prodotto rendendolo non solo usufruibile nel tempo ma anche tecnicamente valido ed affidabile anche per uso pratico