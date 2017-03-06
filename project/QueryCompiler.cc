#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"

using namespace std;


QueryCompiler::QueryCompiler(Catalog& _catalog, QueryOptimizer& _optimizer) :
	catalog(&_catalog), optimizer(&_optimizer) {
	catalog = &_catalog;
	optimizer = &_optimizer;
}

QueryCompiler::~QueryCompiler() {

}

void QueryCompiler::Compile(TableList* _tables, NameList* _attsToSelect,
	FuncOperator* _finalFunction, AndList* _predicate,
	NameList* _groupingAtts, int& _distinctAtts,
	QueryExecutionTree& _queryTree) {
	cout<<"COMPILING.."<<endl;

	//preprocessing 1
        
        TableList* _tableHead = new TableList();
        _tableHead = _tables;
        DBFile _db;
        while(_tableHead != NULL){
            Schema _schema1;
            string _tableName1(_tableHead->tableName);
            catalog->GetSchema(_tableName1, _schema1);
            Scan s(_schema1,_db);
            pair<string,Scan> entry = make_pair(_tableName1,s);
            scanMap.insert (entry);
            _tableHead = _tableHead->next;
        } _tableHead = _tables;
        while (_tableHead != NULL)
        {
            Schema _schema;
            
            string _tableName(_tableHead->tableName);
            catalog->GetSchema(_tableName, _schema);

            CNF cnf;
            Record rec;
            int dov = cnf.ExtractCNF (*_predicate, _schema, rec);

            if (cnf.numAnds != 0)
            {
                string attcomp;

                if(cnf.andList[0].operand1 == Left)
                {
                        vector <Attribute> atri;
                        atri = _schema.GetAtts();
                        attcomp = atri[cnf.andList[0].whichAtt1].name;
                }
                if(cnf.andList[0].operand2 == Left)
                {
                        vector <Attribute> atri;
                        atri = _schema.GetAtts();
                        attcomp = atri[cnf.andList[0].whichAtt2].name;
                }
                if(attcomp != ""){
                    RelationalOp* _producer = (RelationalOp*) & scanMap.at(_tableName);
                    Select sel(_schema, cnf , rec , _producer);
                    //pair<string,Select> tuple = make_pair(_tableName,sel);
                    //selectMap.insert(tuple);
                    selectMap[_tableName] = sel;
                }
                    
            }
            _tableHead = _tableHead->next;
        }

	// call the optimizer to compute the join order
	OptimizationTree* root = new OptimizationTree();
	optimizer->Optimize(_tables, _predicate, root);
	// Creating join operators based on the optimal order computed by the optimizer
        OptimizationTree* rootCopy = root;
        RelationalOp* join = constTree(rootCopy, _predicate);
        
        // creating remaining operators
        if (_finalFunction == NULL) 
        {
            Schema _projectionSchema;
            join->returnSchema(_projectionSchema);
            vector<Attribute> projAtts = _projectionSchema.GetAtts();	

            NameList* attsToSelect = _attsToSelect;
            int numAttsInput = _projectionSchema.GetNumAtts(), numAttsOutput = 0; 
            Schema _projectionSchemaOut = _projectionSchema;
            vector<int> keepMe;

            while (attsToSelect != NULL)
            {
                    string str(attsToSelect->name);
                    keepMe.push_back(_projectionSchema.Index(str));
                    attsToSelect = attsToSelect->next;
                    numAttsOutput++;
            }
            int* keepme = new int [keepMe.size()];
            for (int i = 0;i < keepMe.size(); i++) keepme[i] = keepMe[i]; 

            _projectionSchemaOut.Project(keepMe);
            Project* project = new Project (_projectionSchema, _projectionSchemaOut, numAttsInput, numAttsOutput, keepme, join);

            join = (RelationalOp*) project;

            if (_distinctAtts == 1)
            {
                    Schema dupSch;	
                    join->returnSchema(dupSch);	
                    DuplicateRemoval* duplicateRemoval = new DuplicateRemoval(dupSch, join);
                    join = (RelationalOp*) duplicateRemoval;
            }

        }
        else
        {
                if (_groupingAtts == NULL) 
                {
                        Schema schIn, schIn_;
                        join->returnSchema(schIn_);
                        schIn = schIn_;

                        Function compute;
                        FuncOperator* finalFunction = _finalFunction;
                        compute.GrowFromParseTree(finalFunction, schIn_);

                        vector<string> attributes, attributeTypes;
                        vector<unsigned int> distincts;
                        attributes.push_back("Sum");
                        attributeTypes.push_back("FLOAT");
                        distincts.push_back(1);
                        Schema schOutSum(attributes, attributeTypes, distincts);

                        Sum* sum = new Sum (schIn, schOutSum, compute, join);
                        join = (RelationalOp*) sum;
                }

                else
                {
                        Schema schIn, schIn_;
                        join->returnSchema(schIn_);
                        schIn = schIn_;

                        NameList* grouping = _groupingAtts;
                        int numAtts = 0; 
                        vector<int> keepMe;

                        vector<string> attributes, attributeTypes;
                        vector<unsigned int> distincts;
                        attributes.push_back("Sum");
                        attributeTypes.push_back("FLOAT");
                        distincts.push_back(1);

                        while (grouping != NULL)
                        {
                                string str(grouping->name);
                                keepMe.push_back(schIn_.Index(str));
                                attributes.push_back(str);

                                Type type;
                                type = schIn_.FindType(str);

                                switch(type) 
                                {
                                        case Integer:	attributeTypes.push_back("INTEGER");	break;
                                        case Float:	attributeTypes.push_back("FLOAT");	break;
                                        case String:	attributeTypes.push_back("STRING");	break;
                                        default:	attributeTypes.push_back("UNKNOWN");	break;
                                }

                                distincts.push_back(schIn_.GetDistincts(str));

                                grouping = grouping->next;
                                numAtts++;
                        }

                        int* keepme = new int [keepMe.size()];
                        for (int i = 0; i < keepMe.size(); i++) keepme[i] = keepMe[i];

                        Schema schOut(attributes, attributeTypes, distincts);
                        OrderMaker groupingAtts(schIn_, keepme, numAtts);

                        Function compute;
                        FuncOperator* finalFunction = _finalFunction;
                        compute.GrowFromParseTree(finalFunction, schIn);

                        GroupBy* groupBy = new GroupBy (schIn, schOut, groupingAtts, compute, join);	
                        join = (RelationalOp*) groupBy;
                }	
        }

        Schema finalSchema;
        join->returnSchema(finalSchema);
        string outFile = "out.txt";

        WriteOut * writeout = new WriteOut(finalSchema, outFile, join);
        join = (RelationalOp*) writeout;
        
        //SetRoot
        
        _queryTree.SetRoot(*join);
}
RelationalOp* QueryCompiler::constTree(OptimizationTree* root, AndList* _predicate)
{
	if (root -> leftChild == NULL && root -> rightChild == NULL)
	{	
		//if(checkindex == 1) return (RelationalOp*) & si[0]; 
		RelationalOp* op;
		auto it = selectMap.find(root -> tables[0]);
		if(it != selectMap.end())		op = (RelationalOp*) & it->second;
		else				op = (RelationalOp*) & scanMap.at(it->first);
		
		
		return op;
	}

	if (root -> leftChild -> tables.size() == 1  && root -> rightChild -> tables.size() == 1) 
	{
		string left = root -> leftChild -> tables[0];
		string right = root -> rightChild -> tables[0];

		CNF cnf;
		Schema sch1, sch2;
		RelationalOp* lop, *rop;

		auto it = selectMap.find(left);
		if(it != selectMap.end())		lop = (RelationalOp*) & it->second;
		else				lop = (RelationalOp*) & scanMap.at(left);

		it = selectMap.find(right);
		if(it != selectMap.end()) 	rop = (RelationalOp*) & it->second;
		else				rop = (RelationalOp*) & scanMap.at(right);
	
		lop->returnSchema(sch1);
		rop->returnSchema(sch2);
		
		cnf.ExtractCNF (*_predicate, sch1, sch2);
		Schema schout = sch1;
		schout.Append(sch2);
		Join* join = new Join(sch1, sch2, schout, cnf, lop , rop);
		return ((RelationalOp*) join);
	
	}

	if (root -> leftChild -> tables.size() == 1)
	{	
		string left = root -> leftChild -> tables[0];
		Schema sch1,sch2;
		CNF cnf;		
		RelationalOp* lop;

		auto it = selectMap.find(left);
		if(it != selectMap.end())		lop = (RelationalOp*) & it->second;
		else				lop = (RelationalOp*) & scanMap.at(left);

		lop->returnSchema(sch1);
		RelationalOp* rop = constTree(root -> rightChild, _predicate);
		rop->returnSchema(sch2);

		cnf.ExtractCNF (*_predicate, sch1, sch2);
		Schema schout = sch1;
		schout.Append(sch2);
		Join* join = new Join(sch1, sch2, schout, cnf, lop , rop);
		return ((RelationalOp*) join);
	}

	if (root -> rightChild -> tables.size() == 1)
	{	
		string right = root -> rightChild -> tables[0];
		Schema sch1,sch2;
		CNF cnf;
		RelationalOp* rop;

		auto it = selectMap.find(right);
		if(it != selectMap.end())		rop = (RelationalOp*) & it->second;
		else				rop = (RelationalOp*) & scanMap.at(right);

		rop->returnSchema(sch2);
		RelationalOp* lop = constTree(root -> leftChild, _predicate);
		lop->returnSchema(sch1);
		
		cnf.ExtractCNF (*_predicate, sch1, sch2);
		Schema schout = sch1;
		schout.Append(sch2);
		Join* join = new Join(sch1, sch2, schout, cnf, lop , rop);
		return ((RelationalOp*) join);
	}

	Schema sch1,sch2;
	CNF cnf;
	RelationalOp* lop = constTree(root -> leftChild, _predicate);
	RelationalOp* rop = constTree(root -> rightChild, _predicate);

	lop->returnSchema(sch1);
	rop->returnSchema(sch2);

	cnf.ExtractCNF (*_predicate, sch1, sch2);
	Schema schout = sch1;
	schout.Append(sch2);
	Join* join = new Join(sch1, sch2, schout, cnf, lop , rop);
	return ((RelationalOp*) join);

}
