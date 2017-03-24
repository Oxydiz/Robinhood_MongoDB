#include <stdio.h>
#include <stdlib.h>
#include <bson.h>
#include <bcon.h>
#include <mongoc.h>

//TODO:Use the actual error codes
#define DB_SUCCESS 0
#define DB_REQUEST_FAILED 1
#define DB_END_OF_LIST 2
#define DBOBJ_COLLECTION 10
#define DBOBJ_DOCUMENT 11
#define DBOBJ_DOCUMENTS 12

int mongodb_connect(mongoc_database_t **database, mongoc_client_t ** client, mongoc_collection_t **collection)
{
    bson_t *command,
            reply;
    bson_error_t error;
    bool retval;

    // Create a new client instance
    *client = mongoc_client_new ("mongodb://www.kphamle.com:1234");

    //Get a handle on the database "db_name" and collection "coll_name"
    *database = mongoc_client_get_database(*client, "zobier");
    *collection = mongoc_client_get_collection(*client, "zobier", "mdr");

    //Pinging the database
    command = BCON_NEW ("ping", BCON_INT32 (1));
    retval = mongoc_client_command_simple(*client, "admin", command, NULL, &reply, &error);

    if (!retval)
    {
        fprintf (stderr, "%s\n", error.message);
        return EXIT_FAILURE;
    }

    bson_destroy (&reply);
    bson_destroy (command);
    return 0;
}

static int _mongodb_exec_command(mongoc_client_t *client, mongoc_database_t *database, const bson_t *command, mongoc_cursor_t **result, bool quiet) {

	mongoc_cursor_t *query = mongoc_database_command(database, MONGOC_QUERY_NONE, 0, 0, 0, command, NULL, NULL);
	const bson_t *doc;
	bson_t *doc_copy;
	bson_error_t error;
	int return_code = DB_SUCCESS;
	bson_iter_t iter,sub_iter;

	while(mongoc_cursor_next(query, &doc)) {
		doc_copy = bson_copy(doc);
		if(result != NULL) {
			if(bson_iter_init(&iter,doc))
				if(bson_iter_find(&iter,"cursor"))
					if(bson_iter_recurse(&iter, &sub_iter))
						if(bson_iter_find(&sub_iter,"id")) {
							const bson_value_t *val = bson_iter_value(&iter);
							*result = mongoc_cursor_new_from_command_reply(client,doc_copy,(int)val->value.v_double);
						} else printf("Could not find id field.\n");
					else printf("Sub-iterator initialisation failed.\n");
				else printf("Could not find cursor field.\n");
			else printf("Iterator initialisation failed.\n");
		}
	}

	if(mongoc_cursor_error(query, &error)) {
		printf("Error : Domain %d, error %d - %s\n",error.domain, error.code, error.message);
		return_code = DB_REQUEST_FAILED;
	}

	bson_free((bson_t *)doc);
	mongoc_cursor_destroy(query);
	return return_code;

}

int mongodb_result_free(mongoc_database_t *database, mongoc_cursor_t *result) {
	if(mongoc_cursor_is_alive(result))
		mongoc_cursor_destroy(result);
	return DB_SUCCESS;
}

int _mongodb_parse_fields(char *json, char* keytab[], char *outtab[],int tabsize, int strl) {

	int subdocument = 0, read = 0, nbfields = -1;
	unsigned int i = 0, j = 0;
	bool docend = false, overflow = false;
	char **tab = keytab;

	while(!docend)
		switch(json[++i]) {
			case '\0': docend = true; break;
			case '{':
				if(!subdocument && !overflow)
					tab[nbfields] = malloc((strl+1) * sizeof(char));
				subdocument+=2;
			case '}':
				if(--subdocument == -1) {
					docend = true;
					break;
				}
				if(!subdocument) {
					if(!overflow) {
						tab[nbfields][j] = json[i];
						j += (j+1<strl) ? 1 : 0;
						tab[nbfields][j] = '\0';
					}
					tab = (tab == outtab) ? keytab : outtab;
					j = 0;
					break;
				}
			case '"':
				if(!subdocument) {
					if(++read%2) {
						nbfields += (tab == keytab) ? 1 : 0;
                                                overflow = nbfields == tabsize ? true : false;
						if(!overflow)
							tab[nbfields] = malloc((strl+1) * sizeof(char));
					} else {
						if(!overflow)
							tab[nbfields][j] = '\0';
						tab = (tab == outtab) ? keytab : outtab;
						j = 0;
					}
					break;
				}
			default:
				if(read%2 || subdocument)
					if(j < strl && !overflow)
						tab[nbfields][j++]=json[i];
				break;

		}

	nbfields++;
	return overflow ? nbfields * -1 : nbfields;

}

int mongodb_next_record(mongoc_database_t *database, mongoc_cursor_t *result, char *keytab[], char *outtab[], unsigned int outtabsize, unsigned int bufferlength) {

	bson_error_t error;
	int i;
	for(i = 0; i < outtabsize; i++)
		outtab[i] = keytab[i] = NULL;

	const bson_t *entry;

	if(!mongoc_cursor_next(result,&entry)) {
		if(mongoc_cursor_error(result, &error)) {
               		printf("Error : Domain %d, error %d - %s\n",error.domain, error.code, error.message);
                	return DB_REQUEST_FAILED;
        	} else
			return DB_END_OF_LIST;
	}

	char *str = bson_as_json(entry,NULL);
	int size = _mongodb_parse_fields(str,keytab,outtab,outtabsize,bufferlength);
	if(size < 0) {
		printf("Output array too small : size : %u, num_fields : %u\n",outtabsize,size*-1);
	}

	bson_free(str);
	return DB_SUCCESS;

}

//WARNING: Cursor cloning unexplicably fails.
int mongodb_result_nb_records(mongoc_database_t *database, mongoc_cursor_t *result) {

	mongoc_cursor_t *clone = mongoc_cursor_clone(result);
	int nb = 0;
	const bson_t *tmp;
	bson_error_t error;

	while(mongoc_cursor_next(clone,&tmp))
		nb++;
	if(mongoc_cursor_error(clone, &error))
        	printf("Error : Domain %d, error %d - %s\n",error.domain, error.code, error.message);

	mongoc_cursor_destroy(clone);
	return nb;
}

int mongodb_drop_component(mongoc_collection_t *collection, int component, bson_t *selector) {

	bson_error_t error;
	bool rt;
	int remove_single_document = 0;

	switch(component) {
		case DBOBJ_COLLECTION:
			rt = mongoc_collection_drop(collection,&error);
			if(!rt)
				printf("Error : domain %d, error %d - %s\n",error.domain,error.code,error.message);
			return rt ? 1 : 0;
		case DBOBJ_DOCUMENT:
			remove_single_document = 1;
		case DBOBJ_DOCUMENTS:
			rt = mongoc_collection_remove(collection,remove_single_document,selector,NULL,&error);
			if(!rt)
				printf("Error : domain %d, error %d - %s\n",error.domain,error.code,error.message);
			return rt ? 1 : 0;
		default:
			printf("Object type %d is not supported in %s",component,"__func__");
			break;
	}

	return 0;

}

void findAndDisplay(mongoc_database_t *database, mongoc_collection_t *collection, mongoc_client_t *client) {

	bson_t *command = bson_new();
	mongoc_cursor_t *result;

	BSON_APPEND_UTF8(command,"find",mongoc_collection_get_name(collection));
        _mongodb_exec_command(client,database,command,&result,true);

        char *key[10], *value[10];
        int i;
        printf("\nFound %d records\n",mongodb_result_nb_records(database,result));
        while(mongodb_next_record(database,result,key,value,10,256) == DB_SUCCESS) {
                printf("{\n");
		for(i = 0; i < 10; i++)
                        if(key[i] != NULL || value[i] != 0)
                                printf("\t%s : %s\n",key[i],value[i]);
                printf("}\n\n");
        }

	bson_free(command);

}

int main(int argc, char *argv[]) {
    mongoc_collection_t  *collection;
    mongoc_client_t      *client;
    mongoc_database_t    *database;

    /*
    * Required to initialize libmongoc's internals
    */
    mongoc_init ();

    if(mongodb_connect(&database, &client, &collection))
		return EXIT_FAILURE;

    /*
    * Do work. This example lists all the documents in the database.
    */

	mongoc_cursor_t *result;
	findAndDisplay(database, collection, client);
	getchar();
	system("clear");

	bson_t *command = bson_new(), *array = bson_new(), *insert = bson_new();
	BSON_APPEND_UTF8(command,"insert",mongoc_collection_get_name(collection));
	bson_append_array_begin(command, "documents", -1, array);
	bson_append_document_begin(array,"0",-1,insert);
	BSON_APPEND_UTF8(insert,"Insertion","Confirmée");
	BSON_APPEND_UTF8(insert,"String","Test");
	bson_append_document_end(array,insert);
	bson_append_array_end(command,array);

	int i;
	for(i = 0; i < 5; i++)
		_mongodb_exec_command(client,database,command,&result,true);
	bson_free(command);

	findAndDisplay(database,collection,client);
	getchar();
	system("clear");

	command = bson_new();
	BSON_APPEND_UTF8(command,"String","Test");
	mongodb_drop_component(collection,DBOBJ_DOCUMENT,command);
        bson_free(command);

        findAndDisplay(database,collection,client);
        getchar();
        system("clear");

        command = bson_new();
        BSON_APPEND_UTF8(command,"String","Test");
        mongodb_drop_component(collection,DBOBJ_DOCUMENTS,command);


	bson_free(insert);
	bson_free(array);
	bson_free(command);

	findAndDisplay(database, collection, client);
	// Release our handles and clean up libmongoc

	mongoc_collection_destroy (collection);
 	mongoc_database_destroy (database);
   	mongoc_client_destroy (client);
   	mongoc_cleanup ();

    	return 0;
}
