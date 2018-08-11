//
//  dataStructure.hpp
//  MV-OSTM
//
//  Created by Chirag Juyal on 4/5/18.
//  Copyright © 2018 IIT-HYD. All rights reserved.
//

#ifndef dataStructure_hpp
#define dataStructure_hpp

#include <pthread.h>
#include "common.h"
#include <climits>
#include <assert.h>
#include <vector>


using namespace std;

/**
 *
 * Class defines struture of a single version of a Key-Node.
 *
 **/
class version {
    public:
		/*To maintain the timestamp of the version.*/
        int G_ts;
        /*To define the value contained by G_val.*/
        int G_val;
        /*To maintain the list of transactions read this version.*/
        vector<int> *G_rvl = new vector<int>;
};

/**
 *
 * Class defines struture of a single Key-Node.
 *
 **/
class G_node {
    public:
		/*Defines the key of the key-Node.*/
        int G_key;
        /*Defines the status flag of the key-Node.*/
        bool G_mark;
        /*List to define the versions of this key.*/
        vector<version*> *G_vl = new vector<version*>;
        /*Reinterant lock*/
        recursive_mutex lmutex;
        /*next red node*/
        G_node *red_next;
        /*next blue node*/
        G_node *blue_next;
        /*Default constructor to the class.*/
        G_node();
        /*init the node and create a version with key, value*/
        G_node(int key);
        /*Method to compare two G_nodes on the basis of their keys.*/
        bool compareG_nodes(G_node* node);
};

/*
 * HashMap Class Declaration
 */
class HashMap
{
private:
    G_node **htable;
public:
    /*Constructor of the class.*/
    HashMap();
    ~HashMap();
    
    /*Hash Function*/
    int HashFunc(int key);
    
    /*Insert Key-Node in the appropriate position with a default version.*/
    void list_Ins(int L_key, int* L_val, G_node** G_preds, G_node** G_currs, LIST_TYPE lst_type, int L_tx_id);
    
    /*Function to determine preds and curs.*/
    OPN_STATUS list_LookUp(int L_bucket_id, int L_key, G_node** G_preds, G_node** G_currs);
    
    /*Identify the right version of a G_node that is largest but less than current transaction id.*/
    version* find_lts(int L_tx_id, G_node *G_curr);
    
    /*Check for a Key-Node if not then create one.*/
    OPN_STATUS commonLuNDel(int L_tx_id, int L_bucket_id, int L_key, int* L_val, G_node** G_preds, G_node** G_currs);
    
    /*Method to find the closest tuple created by transaction Tj with the largest timestamp smaller than L_tx_id.*/
    bool check_version(int L_tx_id, G_node* G_curr);
    
    /*Method to add a version in the appropriate Key_node version list in sorted order.*/
    void insertVersion(int L_tx_id, int L_val, G_node* key_Node);
    
    /*Delete Element at a key*/
    void list_Del(G_node** preds, G_node** currs);
    
    /*Print the current table contents.*/
    void printHashMap(int L_bucket_id);
};

/*Function to acquire all locks taken during listLookUp().*/
void acquirePredCurrLocks(G_node** G_preds, G_node** G_currs);
/*Function to release all locks taken during listLookUp().*/
void releasePredCurrLocks(G_node** G_preds, G_node** G_currs);
/*This method identifies the conflicts among the concurrent methods of different transactions.*/
OPN_STATUS methodValidation(G_node** G_preds, G_node** G_currs);

#endif /* dataStructure_hpp */
