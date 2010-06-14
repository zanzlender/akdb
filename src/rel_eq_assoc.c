/**
@file rel_eq_assoc.c Provides functions for for relational equivalences regarding associativity
*/
/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor Boston, MA 02110-1301,  USA
 */

#include "rel_eq_assoc.h"

/**
 * @brief Struct cost_eval comparison function
 * @author Dino Laktašić
 * @param const void *a - first value
 * @param const void *b - second value
 * @return int - returns result of comparison
 */
int compare(const void *a, const void *b) {
  return ((*(struct cost_eval_t*)a).value - (*(struct cost_eval_t*)b).value);
}

/**
 * @brief Main function for generating RA expresion according to associativity equivalence rules 
 * @author Dino Laktašić.
 * @param AK_list *list_rel_eq - RA expresion as the AK_list
 * @result AK_list - returns optimised RA expresion as the AK_list
 */
AK_list *AK_rel_eq_assoc(AK_list *list_rel_eq) {
	//int exit_cond[5] = {0};
	int next_cost, i;
	
	//Initialize temporary linked list
	AK_list *temp = (AK_list *)malloc(sizeof(AK_list));
    InitL(temp);
	
	AK_list_elem tmp, temp_elem, temp_elem_prev, temp_elem_next;
	AK_list_elem list_elem_next, list_elem = (AK_list_elem) FirstL(list_rel_eq);

	//Iterate through all the elements of RA linked list
	while (list_elem != NULL) {
		//printf("read > %s\n", list_elem->data);
		
		switch (list_elem->type) {

			case TYPE_OPERATOR:
				printf("\nOPERATOR '%c' SELECTED\n", list_elem->data[0]);
				printf("----------------------\n");
				temp_elem = (AK_list_elem) EndL(temp);
				temp_elem_prev = (AK_list_elem) PreviousL(temp_elem, temp);
				list_elem_next = (AK_list_elem) NextL(list_elem, list_rel_eq);
				
				switch (list_elem->data[0]) {
					//Associativity of set operations: Union and Intersection are associative but Set Difference is not 
					case RO_UNION:
					case RO_INTERSECT:
						if (temp_elem != NULL) {
							if (temp_elem->type == TYPE_OPERAND && temp_elem_prev->type == TYPE_OPERAND && 
								list_elem_next->type == TYPE_OPERAND && (list_elem_next->next)->type == TYPE_OPERATOR &&
								strcmp(list_elem->data, (list_elem_next->next)->data) == 0) {
									
									cost_eval cost[3];
									
									//We can later consider some other options than number of table records 
									//to get heuristic values for table reordering in association construction
									//Getting table rows count requires loop through all rows in table (very expansive)
									cost[0].value = AK_get_num_records(temp_elem->data);
									cost[1].value = AK_get_num_records(temp_elem_prev->data);
									cost[2].value = AK_get_num_records(list_elem_next->data);
									
									strcpy(cost[0].data, temp_elem->data);
									strcpy(cost[1].data, temp_elem_prev->data);
									strcpy(cost[2].data, list_elem_next->data);
									qsort(cost, 3, sizeof(cost_eval), compare);
									
									//printf("ROW_COUNT: (%s) %i rows\n", cost[0].data, cost[0].value);
									//printf("ROW_COUNT: (%s) %i rows\n", cost[1].data, cost[1].value);
									//printf("ROW_COUNT: (%s) %i rows\n", cost[2].data, cost[2].value);
									
									//Change inserted relation to largest table 
									printf("::table_name (%s) in temp list changed to %s\n", temp_elem_prev->data, cost[2].data);
									temp_elem_prev->size = strlen(cost[2].data) + 1;
									memset(temp_elem_prev->data, '\0', MAX_VARCHAR_LENGHT);
									strcpy(temp_elem_prev->data, cost[2].data);
									
									//Change last inserted relation
									printf("::table_name (%s) in temp list changed to %s\n", temp_elem->data, cost[1].data);
									temp_elem->size = strlen(cost[1].data) + 1;
									memset(temp_elem->data, '\0', MAX_VARCHAR_LENGHT);
									strcpy(temp_elem->data, cost[1].data);
									
									//Insert smallest table at the end of temp list
									InsertAtEndL(TYPE_OPERAND, cost[0].data, strlen(cost[0].data) + 1, temp);
									printf("::table_name (%s) inserted in temp list\n", cost[0].data);
									
									//Insert operator
									InsertAtEndL(list_elem->type, list_elem->data, list_elem->size, temp);
									printf("::operator %s inserted in temp list\n", list_elem->data);
									
									list_elem = list_elem->next;
							} else {
								InsertAtEndL(list_elem->type, list_elem->data, list_elem->size, temp);
								printf("::operator %s inserted in temp list\n", list_elem->data);
							}
						} else {
							InsertAtEndL(list_elem->type, list_elem->data, list_elem->size, temp);
							printf("::operator %s inserted in temp list\n", list_elem->data);
						}
						break;
						
					//Associativity of natural join
					case RO_NAT_JOIN:
						//Example: E1 E2 n[L1] E3 n[L2]
						next_cost = i = 0;
						
						//Set tmp to E3 (relation), move tmp for one place to get next natural join operator if exists
						tmp = list_elem_next->next;
						if (temp_elem_prev != NULL) {
							while (tmp != NULL && (tmp->data[0] != RO_NAT_JOIN && tmp->type != TYPE_OPERATOR)) {
								i++;
								tmp = tmp->next;
							}
						}
						//Current operator and condition
						//list_elem >> n, list_elem_next >> [L1]
												
						//Get next operator and condition if exist
						//tmp >> n, tmp->next >> [L2]
						
						//If there is valid move
						if (i == 1) {
							//Check for valid types (don't need to check operator, neither type for 
							//list_elem because it's alreacdy checked in the switch statement)
							if ((list_elem->data[0] == tmp->data[0] && tmp->type == TYPE_OPERATOR) && 
								(list_elem_next->type == (tmp->next)->type)) { 
								cost_eval cost[3];
								
								//read previous two relations and save their rows number to cost_eval struct, 
								//save also table name
								while (temp_elem->type == TYPE_OPERAND) {
									cost[next_cost].value = AK_get_num_records(temp_elem->data);
									strcpy(cost[next_cost].data, temp_elem->data);
									next_cost++;
									temp_elem = (AK_list_elem)PreviousL(temp_elem, temp);
								}
								
								//see comment on the previous operator for getting heuristics values
								//check for relation after natural join operator, if exists save data to cost_eval struct
								//and then sort all three elements ascending (lower index -> less rows in table)
								if ((list_elem_next->next)->type == TYPE_OPERAND) {
									cost[next_cost].value = AK_get_num_records((list_elem_next->next)->data);
									strcpy(cost[next_cost].data, (list_elem_next->next)->data);
									qsort(cost, 3, sizeof(cost_eval), compare);
								}
								
								//if values for all three relations are saved, rearrange tables in list
								if (next_cost-- == 2) {
									temp_elem = (AK_list_elem)EndL(temp);
									while (next_cost < 3) {
										if (temp_elem->type == TYPE_OPERAND) {
											temp_elem->size = strlen(cost[next_cost].data) + 1;
											memset(temp_elem->data, '\0', MAX_VARCHAR_LENGHT);
											strcpy(temp_elem->data, cost[next_cost].data);
											printf("::table_name (%s) in temp list changed to %s\n", temp_elem->data, cost[next_cost].data);
											next_cost++;
										}
										temp_elem = (AK_list_elem)PreviousL(temp_elem, temp);
									}
									//insert final relation
									InsertAtEndL(TYPE_OPERAND, cost[0].data, strlen(cost[0].data) + 1, temp);
									printf("::table_name (%s) inserted in temp list\n", cost[0].data);
										
									next_cost = 1;
								} else {
									next_cost = 0;
								}
							}
						}
						//insert operator
						InsertAtEndL(list_elem->type, list_elem->data, list_elem->size, temp);
						InsertAtEndL(list_elem_next->type, list_elem_next->data, list_elem_next->size, temp);
						printf("::operator %s inserted with attributes (%s) in temp list\n", list_elem->data, list_elem_next->data);
						
						if (next_cost) {
							printf("TMP: (%s), TMP_NEXT: (%s)\n", tmp->data, (tmp->next)->data);
							InsertAtEndL(tmp->type, tmp->data, tmp->size, temp);
							InsertAtEndL((tmp->next)->type, (tmp->next)->data, (tmp->next)->size, temp);
							printf("::operator %s inserted with attributes (%s) in temp list\n", tmp->data, (tmp->next)->data);
							list_elem = tmp->next;
						} else {
							list_elem = list_elem_next;
						}
						break;
					
					case RO_THETA_JOIN:
						//Example: E1 E2 t[F1] E3 t[F2]
						next_cost = -1;
						tmp = list_elem_next->next;
						
						i = 0;
						while (tmp != NULL && tmp->type != TYPE_CONDITION) {
							tmp = tmp->next;
							i++;
						}
						tmp = list_elem_next->next;

						//if exists elements in range
						if (temp_elem_prev != NULL && i == 2) {
							char *cond_attr_first, *cond_attr_second;
							char *tbl_attr_first, *tbl_attr_second, *tbl_attr_third;
							
							//get attributes from condition F1 (E1 E2 t[F1])
							cond_attr_first = NULL;
							if (list_elem_next->type == TYPE_CONDITION) {
								cond_attr_first = (char *)AK_rel_eq_cond_attributes(list_elem_next->data);
							}
							
							//get attributes from condition F2 (t[F2])
							cond_attr_second = NULL;
							if (((tmp->next)->data[0] == RO_THETA_JOIN && (tmp->next)->type == TYPE_OPERATOR) && (tmp->next->next)->type == TYPE_CONDITION) {
								cond_attr_second = (char *)AK_rel_eq_cond_attributes((tmp->next->next)->data);
							}
							
							//get attributes from table E1
							tbl_attr_first = NULL;
							if (temp_elem->type == TYPE_OPERAND) {							
								tbl_attr_first = (char *)AK_rel_eq_get_attributes(temp_elem_prev->data);
							}
							
							//get attributes from table E2
							tbl_attr_second = NULL;
							if (temp_elem->type == TYPE_OPERAND) {
								tbl_attr_second = (char *)AK_rel_eq_get_attributes(temp_elem->data);
							}
							
							//get attributes from table E3
							tbl_attr_third = NULL;
							if (temp_elem->type == TYPE_OPERAND) {
								tbl_attr_third = (char *)AK_rel_eq_get_attributes(tmp->data);
							}
						
							if (cond_attr_first != NULL && cond_attr_second != NULL && 
								tbl_attr_first != NULL && tbl_attr_second != NULL && tbl_attr_third != NULL) {
								
								//attributes from F1 are subset of attributes from E2 
								//and attributes from F1 are subset of attributes from E1, and they are not susbset of E3
								if (AK_rel_eq_is_attr_subset(tbl_attr_first, cond_attr_first) == EXIT_SUCCESS && 
									AK_rel_eq_is_attr_subset(tbl_attr_second, cond_attr_first) == EXIT_SUCCESS &&
									AK_rel_eq_is_attr_subset(tbl_attr_third, cond_attr_first) == EXIT_FAILURE) {
									//first condition check completed successfully
									next_cost++;
								}
								
								//attributes from F2 are subset of attributes from E2 
								//and attributes from F2 are subset of attributes from E3, and they are not susbset of E1
								if (AK_rel_eq_is_attr_subset(tbl_attr_second, cond_attr_second) == EXIT_SUCCESS && 
									AK_rel_eq_is_attr_subset(tbl_attr_third, cond_attr_second) == EXIT_SUCCESS &&
									AK_rel_eq_is_attr_subset(tbl_attr_first, cond_attr_second) == EXIT_FAILURE) {
									//second condition check completed successfully
									next_cost++;
								}
								
								free(cond_attr_first);
								free(tbl_attr_first);
								free(tbl_attr_second);
								free(tbl_attr_third);
							
							} //end of check for NULLs

							if (next_cost) {
								next_cost = 0;
								cost_eval cost[3];
								
								while (temp_elem->type == TYPE_OPERAND) {
									cost[next_cost].value = AK_get_num_records(temp_elem->data);
									strcpy(cost[next_cost].data, temp_elem->data);
									next_cost++;		
									temp_elem = (AK_list_elem)PreviousL(temp_elem, temp);
								}
								
								if (next_cost > 1) {
									//see comment on the previous operator for getting heuristics values
									cost[next_cost].value = AK_get_num_records((list_elem_next->next)->data);
									strcpy(cost[next_cost].data, (list_elem_next->next)->data);
									qsort(cost, 3, sizeof(cost_eval), compare);
									temp_elem = (AK_list_elem)EndL(temp);
									
									next_cost = 1;
									while (next_cost < 3) {
										if (temp_elem->type == TYPE_OPERAND) {
											temp_elem->size = strlen(cost[next_cost].data) + 1;
											memset(temp_elem->data, '\0', MAX_VARCHAR_LENGHT);
											strcpy(temp_elem->data, cost[next_cost].data);
											printf("::table_name (%s) in temp list changed to %s\n", temp_elem->data, cost[next_cost].data);
											next_cost++;
										}
										temp_elem = (AK_list_elem)PreviousL(temp_elem, temp);
									}

									InsertAtEndL(TYPE_OPERAND, cost[0].data, strlen(cost[0].data) + 1, temp);
									printf("::table_name (%s) inserted in temp list\n", cost[0].data);
									next_cost = 1;
								} else {
									next_cost = -1;
								}
							}
						}
						
						InsertAtEndL(list_elem->type, list_elem->data, list_elem->size, temp);
						InsertAtEndL(list_elem_next->type, list_elem_next->data, list_elem_next->size, temp);
						
						if (next_cost > 0) {
							InsertAtEndL((tmp->next)->type,(tmp->next)->data, (tmp->next)->size, temp);
							InsertAtEndL((tmp->next->next)->type, (tmp->next->next)->data, (tmp->next->next)->size, temp);
							printf("::operator %s inserted with attributes (%s) in temp list\n", list_elem->data, list_elem_next->data);
							printf("::operator %s inserted with attributes (%s) in temp list\n", (tmp->next)->data, (tmp->next->next)->data);
							list_elem = tmp->next;
						} else {
							printf("::operator %s inserted with condition (%s) in temp list\n", list_elem->data, list_elem_next->data);
							list_elem = list_elem->next;
						}
						break;
					case RO_PROJECTION:
					case RO_SELECTION:
						InsertAtEndL(list_elem->type, list_elem->data, list_elem->size, temp);
						InsertAtEndL(list_elem_next->type, list_elem_next->data, list_elem_next->size, temp);
						printf("::operator %s inserted with condition (%s) in temp list\n", list_elem->data, list_elem_next->data);
						list_elem = list_elem->next;
						break;
					
					case RO_EXCEPT:
						InsertAtEndL(list_elem->type, list_elem->data, list_elem->size, temp);
						printf("::operator %s inserted in temp list\n", list_elem->data);
						break;
					
					case RO_RENAME:
						InsertAtEndL(list_elem->type, list_elem->data, list_elem->size, temp);
						printf("::operator %s inserted in temp list\n", list_elem->data);
						break;
					
					default: 
						printf("Invalid operator: %s", list_elem->data); 
						break;
				}
				break;
				
			//additional type definition included to distinguish beetween table name and attribute/s
			case TYPE_ATTRIBS:
				//printf("::attribute (%s) inserted in the temp list\n", list_elem->data);
				break;
			
			//additional type definition included to distinguish beetween attribute/s and condition
			case TYPE_CONDITION:
				//printf("::condition (%s) inserted in the temp list\n", list_elem->data);
				break;
				
			case TYPE_OPERAND:
				printf("::table_name (%s) inserted in the temp list\n", list_elem->data);
				InsertAtEndL(TYPE_OPERAND, list_elem->data, list_elem->size, temp);
				break;
				
			default: 
				printf("Invalid type: %s", list_elem->data); 
				break;
		}
		
		list_elem = list_elem->next;
	}
	
	//====================================> IMPROVMENTS <=======================================
	//Recursive RA optimization (need to implement exit condition in place of each operator, ...)
	//If there is no new changes on the list return generated AK_lists
	//int iter_cond;
	//for (iter_cond = 0; iter_cond < sizeof(exit_cond); iter_cond++) {
	//	if (exit_cond[iter_cond] == 0) {
	////	Edit function to return collection of the AK_lists
	////	Generate next RA expr. (new plan)
	////	temp += remain from the list_rel_eq
	//		AK_rel_eq_assoc(temp);	
	//	}
	//}
	
	DeleteAllL(list_rel_eq);
	return temp;	
}

void AK_print_rel_eq_assoc(AK_list *list_rel_eq) {
	AK_list_elem list_elem = (AK_list_elem)FirstL(list_rel_eq);

	printf("\n");
	while (list_elem != NULL) {
		printf("Type: %i, size: %i, data: %s\n", list_elem->type, list_elem->size, list_elem->data);
		list_elem = list_elem->next;
	}
}

void rel_eq_assoc_test() {
	printf("rel_eq_assoc.c: Present!\n");
	printf( "\n********** REL_EQ_ASSOCIATIVITY TEST by Dino Laktašić **********\n");
	
	//create header
    AK_header t_header[MAX_ATTRIBUTES];
    AK_header *temp;

    temp = (AK_header*) AK_create_header("id", TYPE_INT, FREE_INT, FREE_CHAR, FREE_CHAR);
    memcpy(t_header, temp, sizeof(AK_header));
    temp = (AK_header*) AK_create_header("firstname", TYPE_VARCHAR, FREE_INT, FREE_CHAR, FREE_CHAR);
    memcpy(t_header + 1, temp, sizeof(AK_header));
    temp = (AK_header*) AK_create_header("job", TYPE_VARCHAR, FREE_INT, FREE_CHAR, FREE_CHAR);
    memcpy(t_header + 2, temp, sizeof(AK_header));
	temp = (AK_header*) AK_create_header("year", TYPE_INT, FREE_INT, FREE_CHAR, FREE_CHAR);
    memcpy(t_header + 3, temp, sizeof(AK_header));
    temp = (AK_header*) AK_create_header("weight", TYPE_FLOAT, FREE_INT, FREE_CHAR, FREE_CHAR);
    memcpy(t_header + 4, temp, sizeof(AK_header));
	memset(t_header + 5, '\0', MAX_ATTRIBUTES - 5);
    
	//create table
    char *tblName = "profesor";
    
    int startAddress = AK_initialize_new_segment(tblName, SEGMENT_TYPE_TABLE, t_header);
    
    if (startAddress != EXIT_ERROR)
        printf("\nTABLE %s CREATED!\n", tblName);
    
    printf("rel_eq_associativity_test: After segment initialization: %d\n", AK_num_attr(tblName));
	
	element row_root = (element)malloc(sizeof(list));
    InitializeList(row_root);
    
    int id = 35890, year = 1999;
    float weight = 80.00;

    //insert rows in table student
    id++; year++; weight += 0.75;
    DeleteAllElements(row_root);
    InsertNewElement( TYPE_INT, &id, tblName, "id", row_root );
    InsertNewElement( TYPE_VARCHAR, "Mirko", tblName, "firstname", row_root );
    InsertNewElement( TYPE_VARCHAR, "Sestak", tblName, "job", row_root );
    InsertNewElement( TYPE_INT, &year, tblName, "year", row_root );
    InsertNewElement( TYPE_FLOAT, &weight, tblName, "weight", row_root );
    insert_row( row_root );

    id++; year++;weight += 0.75;
    DeleteAllElements(row_root);
    InsertNewElement( TYPE_INT, &id, tblName, "id", row_root );
    InsertNewElement( TYPE_VARCHAR, "Igor", tblName, "firstname", row_root);
    InsertNewElement( TYPE_VARCHAR, "Mesaric", tblName, "job", row_root );
    InsertNewElement( TYPE_INT, &year, tblName, "year", row_root );
    InsertNewElement( TYPE_FLOAT, &weight, tblName, "weight", row_root );
    insert_row( row_root );

    id++; year++;weight += 0.75;
    DeleteAllElements(row_root);
    InsertNewElement( TYPE_INT, &id, tblName, "id", row_root );
    InsertNewElement( TYPE_VARCHAR, "Dalibor", tblName, "firstname", row_root);
    InsertNewElement( TYPE_VARCHAR, "Slunjski", tblName, "job", row_root );
    InsertNewElement( TYPE_INT, &year, tblName, "year", row_root );
    InsertNewElement( TYPE_FLOAT, &weight, tblName, "weight", row_root );
    insert_row( row_root );

    id++; year++;weight += 0.75;
    DeleteAllElements(row_root);
    InsertNewElement( TYPE_INT, &id, tblName, "id", row_root );
    InsertNewElement( TYPE_VARCHAR, "Dino", tblName, "firstname", row_root);
    InsertNewElement( TYPE_VARCHAR, "Alagic", tblName, "job", row_root );
    InsertNewElement( TYPE_INT, &year, tblName, "year", row_root );
    InsertNewElement( TYPE_FLOAT, &weight, tblName, "weight", row_root );
    insert_row( row_root );

    id++; year++;weight += 0.75;
    DeleteAllElements(row_root);
    InsertNewElement( TYPE_INT, &id, tblName, "id", row_root );
    InsertNewElement( TYPE_VARCHAR, "Miroslav", tblName, "firstname", row_root );
    InsertNewElement( TYPE_VARCHAR, "Zver", tblName, "job", row_root );
    InsertNewElement( TYPE_INT, &year, tblName, "year", row_root );
    InsertNewElement( TYPE_FLOAT, &weight, tblName, "weight", row_root );
    insert_row( row_root );

    id++; year++;weight += 0.75;
    DeleteAllElements(row_root);
    InsertNewElement( TYPE_INT, &id, tblName, "id", row_root );
    InsertNewElement( TYPE_VARCHAR, "Josip", tblName, "firstname", row_root );
    InsertNewElement( TYPE_VARCHAR, "Vincek", tblName, "job", row_root );
    InsertNewElement( TYPE_INT, &year, tblName, "year", row_root );
    InsertNewElement( TYPE_FLOAT, &weight, tblName, "weight", row_root );
    insert_row( row_root );

    printf("rel_eq_associativity_test: After data insertion: %d\n", AK_num_attr(tblName));
	
	//create header
    AK_header t_header2[MAX_ATTRIBUTES];
    AK_header *temp2;

    temp2 = (AK_header*) AK_create_header("mbr", TYPE_INT, FREE_INT, FREE_CHAR, FREE_CHAR);
    memcpy(t_header2, temp2, sizeof(AK_header));
    temp2 = (AK_header*) AK_create_header("firstname", TYPE_VARCHAR, FREE_INT, FREE_CHAR, FREE_CHAR);
    memcpy(t_header2 + 1, temp2, sizeof(AK_header));
    temp2 = (AK_header*) AK_create_header("lastname", TYPE_VARCHAR, FREE_INT, FREE_CHAR, FREE_CHAR);
    memcpy(t_header2 + 2, temp2, sizeof(AK_header));
	temp2 = (AK_header*) AK_create_header("year", TYPE_INT, FREE_INT, FREE_CHAR, FREE_CHAR);
    memcpy(t_header2 + 3, temp2, sizeof(AK_header));
    temp2 = (AK_header*) AK_create_header("weight", TYPE_FLOAT, FREE_INT, FREE_CHAR, FREE_CHAR);
    memcpy(t_header2 + 4, temp2, sizeof(AK_header));
	memset(t_header2 + 5, '\0', MAX_ATTRIBUTES - 5);
    
	//create table
    char *tblName2 = "student";
    
    int startAddress2 = AK_initialize_new_segment(tblName2, SEGMENT_TYPE_TABLE, t_header2);
    
    if (startAddress2 != EXIT_ERROR)
        printf("\nTABLE %s CREATED!\n", tblName2);
    
    printf("rel_eq_associativity_test: After segment initialization: %d\n", AK_num_attr(tblName2));
	
	element row_root2 = (element)malloc(sizeof(list));
    InitializeList(row_root2);
    
    int mbr = 35890;
	year = 1999;
	weight = 80.00;

    //insert rows in table student
    mbr++; year++; weight += 0.75;
    DeleteAllElements(row_root2);
    InsertNewElement( TYPE_INT, &mbr, tblName2, "mbr", row_root2 );
    InsertNewElement( TYPE_VARCHAR, "Matija", tblName2, "firstname", row_root2 );
    InsertNewElement( TYPE_VARCHAR, "Sestak", tblName2, "lastname", row_root2 );
    InsertNewElement( TYPE_INT, &year, tblName2, "year", row_root2 );
    InsertNewElement( TYPE_FLOAT, &weight, tblName2, "weight", row_root2 );
    insert_row( row_root2 );

    mbr++; year++;weight += 0.75;
    DeleteAllElements(row_root2);
    InsertNewElement( TYPE_INT, &mbr, tblName2, "mbr", row_root2 );
    InsertNewElement( TYPE_VARCHAR, "Igor", tblName2, "firstname", row_root2);
    InsertNewElement( TYPE_VARCHAR, "Mesaric", tblName2, "lastname", row_root2 );
    InsertNewElement( TYPE_INT, &year, tblName2, "year", row_root2 );
    InsertNewElement( TYPE_FLOAT, &weight, tblName2, "weight", row_root2 );
    insert_row( row_root2 );

    mbr++; year++;weight += 0.75;
    DeleteAllElements(row_root2);
    InsertNewElement( TYPE_INT, &mbr, tblName2, "mbr", row_root2 );
    InsertNewElement( TYPE_VARCHAR, "Dalibor", tblName2, "firstname", row_root2);
    InsertNewElement( TYPE_VARCHAR, "Slunjski", tblName2, "lastname", row_root2 );
    InsertNewElement( TYPE_INT, &year, tblName2, "year", row_root2 );
    InsertNewElement( TYPE_FLOAT, &weight, tblName2, "weight", row_root2 );
    insert_row( row_root2 );

    mbr++; year++;weight += 0.75;
    DeleteAllElements(row_root2);
    InsertNewElement( TYPE_INT, &mbr, tblName2, "mbr", row_root2 );
    InsertNewElement( TYPE_VARCHAR, "Dino", tblName2, "firstname", row_root2);
    InsertNewElement( TYPE_VARCHAR, "Alagic", tblName2, "lastname", row_root2 );
    InsertNewElement( TYPE_INT, &year, tblName2, "year", row_root2 );
    InsertNewElement( TYPE_FLOAT, &weight, tblName2, "weight", row_root2 );
    insert_row( row_root2 );

    mbr++; year++;weight += 0.75;
    DeleteAllElements(row_root2);
    InsertNewElement( TYPE_INT, &mbr, tblName2, "mbr", row_root2 );
    InsertNewElement( TYPE_VARCHAR, "Miroslav", tblName2, "firstname", row_root2 );
    InsertNewElement( TYPE_VARCHAR, "Zver", tblName2, "lastname", row_root2 );
    InsertNewElement( TYPE_INT, &year, tblName2, "year", row_root2 );
    InsertNewElement( TYPE_FLOAT, &weight, tblName2, "weight", row_root2 );
    insert_row( row_root2 );

    mbr++; year++;weight += 0.75;
    DeleteAllElements(row_root2);
    InsertNewElement( TYPE_INT, &mbr, tblName2, "mbr", row_root2 );
    InsertNewElement( TYPE_VARCHAR, "Josip", tblName2, "firstname", row_root2 );
    InsertNewElement( TYPE_VARCHAR, "Vincek", tblName2, "lastname", row_root2 );
    InsertNewElement( TYPE_INT, &year, tblName2, "year", row_root2 );
    InsertNewElement( TYPE_FLOAT, &weight, tblName2, "weight", row_root2 );
    insert_row( row_root2 );
    
    mbr++; year++;weight += 0.75;
    DeleteAllElements(row_root2);
    InsertNewElement( TYPE_INT, &mbr, tblName2, "mbr", row_root2 );
    InsertNewElement( TYPE_VARCHAR, "Netko", tblName2, "firstname", row_root2 );
    InsertNewElement( TYPE_VARCHAR, "Netkic", tblName2, "lastname", row_root2 );
    InsertNewElement( TYPE_INT, &year, tblName2, "year", row_root2 );
    InsertNewElement( TYPE_FLOAT, &weight, tblName2, "weight", row_root2 );
    insert_row( row_root2 );

    mbr++; year++;weight += 0.75;
    DeleteAllElements(row_root2);
    InsertNewElement( TYPE_INT, &mbr, tblName2, "mbr", row_root2 );
    InsertNewElement( TYPE_VARCHAR, "Dino", tblName2, "firstname", row_root2 );
    InsertNewElement( TYPE_VARCHAR, "Laktašić", tblName2, "lastname", row_root2 );
    InsertNewElement( TYPE_INT, &year, tblName2, "year", row_root2 );
    InsertNewElement( TYPE_FLOAT, &weight, tblName2, "weight", row_root2 );
    insert_row( row_root2 );

    printf("rel_eq_associativity_test: After data insertion: %d\n", AK_num_attr(tblName2));
	//-----------------------------------------------------------------------------------------
	 
	AK_list *expr = (AK_list *)malloc(sizeof(AK_list));
    InitL(expr);
	
	InsertAtEndL( TYPE_OPERATOR, "p", sizeof("p"), expr );
    InsertAtEndL( TYPE_ATTRIBS, "L1;L2;L3;L4", sizeof("L1;L2;L3;L4"), expr ); //projection attribute
	InsertAtEndL( TYPE_OPERATOR, "p", sizeof("p"), expr );
    InsertAtEndL( TYPE_ATTRIBS, "L1;L4;L3;L2;L5", sizeof("L1;L4;L3;L2;L5"), expr );
    InsertAtEndL( TYPE_OPERATOR, "s", sizeof("s"), expr );
	InsertAtEndL( TYPE_CONDITION, "`L1` > 100 OR `L2` < 50", sizeof("`L1` > 100 OR `L2` < 50"), expr );
    InsertAtEndL( TYPE_OPERAND, "student", sizeof("student"), expr );
    InsertAtEndL( TYPE_OPERAND, "profesor", sizeof("profesor"), expr );
	InsertAtEndL( TYPE_OPERATOR, "u", sizeof("u"), expr );
	InsertAtEndL( TYPE_OPERAND, "student", sizeof("student"), expr );
	InsertAtEndL( TYPE_OPERATOR, "u", sizeof("u"), expr );
	InsertAtEndL( TYPE_OPERATOR, "p", sizeof("p"), expr );
    InsertAtEndL( TYPE_ATTRIBS, "mbr;firstname;job", sizeof("mbr;firstname;job"), expr );
	InsertAtEndL( TYPE_OPERAND, "student", sizeof("student"), expr );
    InsertAtEndL( TYPE_OPERAND, "profesor", sizeof("profesor"), expr );
	InsertAtEndL( TYPE_OPERATOR, "t", sizeof("t"), expr );
    InsertAtEndL( TYPE_CONDITION, "`mbr` = `id`", sizeof("`mbr` = `id`"), expr ); //theta join attribute
	
	InsertAtEndL( TYPE_OPERAND, "student", sizeof("student"), expr );
	InsertAtEndL( TYPE_OPERAND, "profesor", sizeof("profesor"), expr );
    InsertAtEndL( TYPE_OPERATOR, "n", sizeof("n"), expr );
	InsertAtEndL( TYPE_OPERAND, "mbr;job", sizeof("mbr;job"), expr );
    InsertAtEndL( TYPE_OPERAND, "profesor", sizeof("profesor"), expr );
	InsertAtEndL( TYPE_OPERATOR, "n", sizeof("n"), expr );
	InsertAtEndL( TYPE_CONDITION, "mbr;job", sizeof("mbr;job"), expr );
	
	//printf("\nRA expr. before rel_eq optimization:\n");
	//AK_print_rel_eq_projection(expr);
    AK_print_rel_eq_assoc(AK_rel_eq_assoc(expr));
	
	printf("\n------------------> TEST_ASSOCIATIVITY_FUNCTIONS <------------------\n\n");
    printf("...");
	
	DeleteAllL(expr);
    //dealocate variables ;)
}