

#include "dataStructure.hpp"

/*
 * Constructor of the class G_node.
 */
G_node::G_node(int key)
{
	this->L_bucket_id =  (key % TABLE_SIZE);
    this->G_key = key;
    this->red_next = NULL;
    this->blue_next = NULL;
    this->G_mark = DEFAULT_MARKED;
    //push the default version back to the G_node.
    version *T0_version = new version;
    T0_version->G_ts = 0;
    T0_version->G_val = 0;
    this->G_vl->push_back(T0_version);
    memory_count++;
}
/*
 * Method to compare two G_nodes on the basis of their keys.
 */
bool G_node::compareG_nodes(G_node* node)
{
    if(this->L_bucket_id == node->L_bucket_id && this->G_key == node->G_key)
    {
        return true;
    }
    return false;
}

/************************************************************************************************************/

/*
 * Constructor of the class HashMap.
 */
HashMap::HashMap()
{
    htable = new G_node* [TABLE_SIZE];
    //Initialize head and tail sentinals nodes for 
    // all the buckets of HashMap.
    for (int i = 0; i < TABLE_SIZE; i++)
    {
        //htable[i] = NULL;
        htable[i] = new G_node(INT_MIN);
        htable[i]->L_bucket_id = i;
        htable[i]->red_next = new G_node(INT_MAX);
        htable[i]->red_next->L_bucket_id = i;
        htable[i]->blue_next = htable[i]->red_next;
    }
}

/*
 * Destructor of the class HashMap.
 */
HashMap::~HashMap()
{
    for (int i = 0; i < TABLE_SIZE; i++)
    {
        if (htable[i] != NULL)
        {
            G_node *prev = NULL;
            G_node *entry = htable[i];
            while (entry != NULL)
            {
                prev = entry;
                entry = entry->red_next;
                delete prev;
            }
        }
    }
    delete[] htable;
}

/*
 * Function to define the object Id or bucket number.
 */
int HashMap::HashFunc(int key)
{
    return key % TABLE_SIZE;
}

/*
 *Insert Key-Node in the appropriate position with a default version.
 */
void HashMap::list_Ins(int L_key, int *L_val, G_node** G_preds, G_node** G_currs, LIST_TYPE list_type, int L_tx_id)
{
    //Inserting the node from redList to blueList
    if(RL_BL == list_type)
    {
        G_currs[0]->blue_next = G_currs[1];
        G_preds[0]->blue_next = G_currs[0];
        G_currs[0]->G_mark = false;
        assert(L_key == G_currs[0]->G_key);
    }
    //Inserting the node in redlist only.
    else if(RL == list_type)
    {
        G_node *node = new G_node(L_key);
        //After creating node lock it.
        node->lmutex.lock();
        /*Add current transaction to the rv-list of the 0th version.*/
        node->G_vl->at(0)->G_rvl->push_back(L_tx_id);
        node->G_mark = true;
        node->red_next = G_currs[0];
        G_currs[0]->lmutex.unlock();
        G_currs[0] = node;
        G_preds[1]->red_next = node;
    }
    //Inserting the node in red as well as blue list.
    else
    {
        G_node *node = new G_node(L_key);
        //After creating node lock it.
        node->lmutex.lock();
        //Add another version with current transaction's timestamp.
        version *T_key_version = new version;
        T_key_version->G_ts = L_tx_id;
        if(L_val != NULL)
            T_key_version->G_val = *L_val;
        else
            T_key_version->G_val = -1;
         memory_count++;
        /*Push the current timestamp version to the node's RV list.*/
        node->G_vl->push_back(T_key_version);
        node->red_next = G_currs[0];
        node->blue_next = G_currs[1];
        G_preds[1]->red_next = node;
        G_preds[0]->blue_next = node;
    }
}

/*
 * Funtion to determine preds and currs.
 */
OPN_STATUS HashMap::list_LookUp(int L_bucket_id, int L_key, G_node** G_preds, G_node** G_currs)
{
    OPN_STATUS op_status = RETRY;
    G_node *head = NULL;
    
    //If key to be searched is not in the range between -infinity to +infinity.
    if((L_key <= INT_MIN) && (L_key >= INT_MAX))
    {
        assert((L_key > INT_MIN) && (L_key < INT_MAX));
    }
    
    //Run until status of operation doesn't change from RETRY.
    while(RETRY == op_status)
    {
        /*Get the head of the bucket in chaining hash-table with the help of L_obj_id and L_key.*/
        //if bucket is empty
        if (htable[L_bucket_id] == NULL)
        {
            elog("bucket is empty \n");
            return BUCKET_EMPTY;
        }
        else
        {
            head = htable[L_bucket_id];
        }
        
        G_preds[0] = head;
        G_currs[1] = G_preds[0]->blue_next;
        
        //search blue pred and curr
        while(G_currs[1]->G_key < L_key)
        {
            G_preds[0] = G_currs[1];
            G_currs[1] = G_currs[1]->blue_next;
        }
        G_preds[1] = G_preds[0];
        G_currs[0] = G_preds[0]->red_next;
        
        //search red pred and curr
        while(G_currs[0]->G_key < L_key)
        {
            G_preds[1] = G_currs[0];
            G_currs[0] = G_currs[0]->red_next;
        }
        
        //Acquire locks on all preds and currs.
        acquirePredCurrLocks(G_preds, G_currs);

        //validation
        op_status = methodValidation(G_preds, G_currs);
        
        //If op_status is still RETRY and not OK release all the laquired locks and try again.
        if(RETRY == op_status)
        {
            releasePredCurrLocks(G_preds, G_currs);
        }
    }
    
    #if DEBUG_LOGS
    cout<< endl<<"list_LookUp:: nodes " <<G_preds[0]->G_key<<" "<< G_preds[1]->G_key<<" "<<G_currs[0]->G_key<<" "<<G_currs[1]->G_key<<endl;
    #endif // DEBUG_LOGS
    return op_status;
}

/*
 * Method to do insert sanity check.
 */
bool HashMap::insert_sanity(int L_tx_id, int L_bucket_id, int L_key)
{
	G_node *G_preds[2];
    G_node *G_currs[2];
    //Operation status to be returned.
    OPN_STATUS L_opn_status;
    /*If node corresponding to the key is not present in local log then search
     into underlying DS with the help of listLookUp().*/
    L_opn_status = list_LookUp(L_bucket_id,L_key,G_preds,G_currs);
	
	/*If node corresponding to the key is part of BL.*/
    if(G_currs[1]->G_key == L_key)
    {
        version *closest_tuple = find_lts(L_tx_id,G_currs[1]);
        
        //Set the max_rvl value accordingly.
        if(closest_tuple->max_rvl > L_tx_id)
		{
			/*Releasing the locks in the incresing order.*/
			releasePredCurrLocks(G_preds, G_currs);
			return false;
		}
    }
    /*If node corresponding to the key is part of RL.*/
    else if(G_currs[0]->G_key == L_key)
    {
        version *closest_tuple = find_lts(L_tx_id,G_currs[0]);
        
        //Set the max_rvl value accordingly.
        if(closest_tuple->max_rvl > L_tx_id)
        {
			/*Releasing the locks in the incresing order.*/
			releasePredCurrLocks(G_preds, G_currs);
			return false;
		}
    }
    
    /*Releasing the locks in the incresing order.*/
    releasePredCurrLocks(G_preds, G_currs);
    return true;;
}


/*
 * Identify the right version of a G_node that is largest but less than current transaction id.
 */
version* HashMap::find_lts(int L_tx_id, G_node *G_curr)
{
	bool flag = false;
    /*Initialize the closest tuple.*/
    version *closest_tuple = new version();
    closest_tuple->G_ts = 0;
    
    /*For all the versions of G_curres[] identify the largest timestamp less than L_tx_id.*/
    for(int i=0; i<G_curr->G_vl->size();i++)
    {
        int p = G_curr->G_vl->at(i)->G_ts;
        if((p < L_tx_id) && (closest_tuple->G_ts <= p))
        {
			closest_tuple = G_curr->G_vl->at(i);
			flag = true;
        }
    }
    //If no version is find to be read from.
    if(flag == false) 
    {
		closest_tuple = NULL;
	}
    return closest_tuple;
}

/*
 * This method does below steps ->
 * 1. Check if the key exists or not, if yes then check for the LTS version to read from.
 * 2. Else create the key and its respective version to be read from.
 * 3. If version to be read from is found then add current transaction to its RV list.
 */

OPN_STATUS HashMap:: commonLuNDel(int L_tx_id, int L_bucket_id, int L_key, int* L_val, G_node** G_preds, G_node** G_currs)
{
    //Operation status to be returned.
    OPN_STATUS L_opn_status;
    /*If node corresponding to the key is not present in local log then search
     into underlying DS with the help of listLookUp().*/
    L_opn_status = list_LookUp(L_bucket_id,L_key,G_preds,G_currs);
    
    /*If node corresponding to the key is part of BL.*/
    if(G_currs[1]->G_key == L_key)
    {
       if(G_currs[1]->G_vl->at(0)->G_ts > L_tx_id)
		{
			//Releasing the locks in the incresing order.
			releasePredCurrLocks(G_preds, G_currs);
			return ABORT;
		}
		
        version *closest_tuple = find_lts(L_tx_id,G_currs[1]);
        if(closest_tuple == NULL)
        { 
			//Releasing the locks in the incresing order.
			releasePredCurrLocks(G_preds, G_currs);
			return ABORT;
		}
        // Push the read entry of tranx to the rvl of this version.
        closest_tuple->G_rvl->push_back(L_tx_id);
        //Set the max_rvl value accordingly.
        if(closest_tuple->max_rvl < L_tx_id) 
			closest_tuple->max_rvl = L_tx_id;

        /*If the Key-Node mark field is TRUE then L_op_status and L_val set as
          FAIL and NULL otherwise set OK and value of closest_tuple respectively.*/
        if(G_currs[1]->G_mark == true)
        {
            L_opn_status = FAIL;
            L_val = NULL;
        } else {
            L_opn_status = OK;
            L_val = &(closest_tuple->G_val);
        }
    }
    /*If node corresponding to the key is part of RL.*/
    else if(G_currs[0]->G_key == L_key)
    {
        if(G_currs[0]->G_vl->at(0)->G_ts > L_tx_id)
		{
			//Releasing the locks in the incresing order.
			releasePredCurrLocks(G_preds, G_currs);	
			return ABORT;
		}
        version *closest_tuple = find_lts(L_tx_id,G_currs[0]);
        if(closest_tuple == NULL)
        {
			//Releasing the locks in the incresing order.
			releasePredCurrLocks(G_preds, G_currs);
			return ABORT;
		}
        // Push the read entry of tranx to the rvl of this version.
        closest_tuple->G_rvl->push_back(L_tx_id);
        //Set the max_rvl value accordingly.
        if(closest_tuple->max_rvl < L_tx_id) {
			closest_tuple->max_rvl = L_tx_id;
		}
        
        /*If the Key-Node mark field is TRUE then L_op_status and L_val set as
         FAIL and NULL otherwise set OK and value of closest_tuple respectively.*/
        if(G_currs[0]->G_mark == true)
        {
            L_opn_status = FAIL;
            L_val = NULL;
        } else {
            L_opn_status = OK;
            L_val = &(closest_tuple->G_val);
        }
    }
    /*If node corresponding to the key is not part of RL as well as BL then create
     the node into RL with the help of list_Ins().*/
    else {
        /*Insert the version tuple for transaction T0.*/
        list_Ins(L_key, L_val, G_preds, G_currs, RL, L_tx_id);
        L_opn_status = FAIL;
        L_val = NULL;
    }
    
    /*Releasing the locks in the incresing order.*/
    releasePredCurrLocks(G_preds, G_currs);
    return L_opn_status;
}

/*
 *Method to find the closest tuple created by transaction Tj with the largest timestamp smaller than L_ix_id.
 */
bool HashMap::check_version(int L_tx_id, G_node* G_curr)
{
    //Identify the tuple that has highest timestamp but less than itself.
    if(G_curr->G_vl->at(0)->G_ts > L_tx_id)
		 return false;
    version *closest_tuple = find_lts(L_tx_id,G_curr);
    if(closest_tuple == NULL)
    {
			return false;
	}
    if(closest_tuple != NULL)
    {
        // If in RV list there exists a transaction with higher ts then the current transaction then return false;
		if(closest_tuple->max_rvl > L_tx_id)
			return false;   
    }
    // If in the rv list all the transactions are of lower ts then the current transaction then return true.
    return true;
}

/*
 * Method to add a version in the appropriate key_node version list in sorted order.
 */
void HashMap::insertVersion(int L_tx_id, int L_val, G_node* key_Node)
{
    version *newVersion = new version();
    newVersion->G_ts = L_tx_id;
    newVersion->G_val = L_val;
    
    /*If the size of the versionlist is less than K, then insert the 
    version in the appropriate order in the version list.*/
    if(key_Node->G_vl->size() < K) {
		for(int i=0;i<key_Node->G_vl->size();i++)
		{
			/*Look for an appropriate place to put the new version in 
			 * orer to maintain the order of the version list.*/
			if(key_Node->G_vl->at(i)->G_ts > L_tx_id)
			{
				key_Node->G_vl->insert(key_Node->G_vl->begin()+i, newVersion);
				memory_count++;
				return;
			}
		}
	} 
	/*If the version list is of size K or more than first delete the 
	 * first version of the list and then insert the new version in the 
	 * appropriate order in the version list.*/
	else {
		key_Node->G_vl->erase(key_Node->G_vl->begin());
		memory_count--;
		for(int i=0;i<key_Node->G_vl->size();i++)
		{
			/*Look for an appropriate place to put the new version 
			 * in orer to maintain the order of the version list.*/
			if(key_Node->G_vl->at(i)->G_ts > L_tx_id)
			{
				key_Node->G_vl->insert(key_Node->G_vl->begin()+i, newVersion);
				memory_count++;
				return;
			}
		}
	}
    /*Else push at the last.*/
    key_Node->G_vl->push_back(newVersion);
    memory_count++;
}

/*
 *Function to delete a node from blue link and place it in red link after marking it.
 */
void HashMap::list_Del(G_node** G_preds, G_node** G_currs)
{
    G_preds[0]->blue_next = G_currs[1]->blue_next;
}

/**
 * Print the current table contents.
 **/
void HashMap::printHashMap(int L_bucket_id)
{
	G_node *head = NULL;
	
	if (htable[L_bucket_id] == NULL)
    {
       elog("bucket is empty \n");
       return;
    }
    else
    {
       head = htable[L_bucket_id];
    }
       
    while(head->G_key < INT_MAX)
    {
       cout<<"Key "<<head->G_key<<" ";
       for(int i=0;i<head->G_vl->size();i++)
       {
		   cout<<head->G_vl->at(i)->G_ts<<" ";
	   }
	   cout<<endl;
       head = head->red_next;
    }
}
/************************************************************************************************************/

/*
 *Function to acquire all locks taken during listLookUp().
 */
void acquirePredCurrLocks(G_node** G_preds, G_node** G_currs)
{
		G_preds[0]->lmutex.lock();
		G_preds[1]->lmutex.lock();
		G_currs[0]->lmutex.lock();
		G_currs[1]->lmutex.lock();

}

/*
 *Function to release all locks taken during listLookUp().
 */
void releasePredCurrLocks(G_node** G_preds, G_node** G_currs)
{
	G_preds[0]->lmutex.unlock();
	G_preds[1]->lmutex.unlock();
	G_currs[0]->lmutex.unlock();
	G_currs[1]->lmutex.unlock();
}

/*
 *This method identifies the conflicts among the concurrent methods of different transactions.
 */
OPN_STATUS methodValidation(G_node** G_preds, G_node** G_currs)
{
    /*Validating g_pred[] and G_currs[].*/
    if((G_preds[0]->G_mark) || (G_currs[1]->G_mark) || (G_preds[0]->blue_next != G_currs[1]) || (G_preds[1]->red_next != G_currs[0]))
    {
        return RETRY;
    }
    else
        return OK;
}
