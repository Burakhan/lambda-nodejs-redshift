var AWS = require('aws-sdk');
    // Set the Region
AWS.config.update({region: 'us-east-1', redshift: '2012-12-01' });
var redshiftdata = new AWS.RedshiftData(); 
/**
 * Convert result data to json object
 */
var convertResultToJson = (data) => {
   let column = [];
   let rows = [];
   let tmp = {};
   data.ColumnMetadata.forEach(c => column.push(c.name));
   
   data.Records.forEach(item => {
      item.forEach((v,i) => {
        tmp[column[i]] = Object.values(v).shift();
      });
      rows.push(tmp);
      tmp = {};
   });
   return rows;
}
 /**
 * Is check statement status Finished
 */
var checkStatusAndResult = (params) => {
 return new Promise(async (resolve, reject) => {
    while(true) {
       try {
          var describeStateResult = await redshiftdata.describeStatement(params).promise();
        } catch (e) {
           reject(e);
        }
        if(describeStateResult.Status == 'FINISHED') {
          try {
            var getStateResult = await redshiftdata.getStatementResult(params).promise();
          } catch (e) {
            reject(e);
            console.log(e, e.stack);
          }
          console.log(JSON.stringify(getStateResult));
          resolve(convertResultToJson(getStateResult));
          break;
        }
    }
 });
};
    
exports.handler = async (event, context, callback) => {
    
    /**
     * Start Create Table Example
     **/
    var tableName = 'Employee';
    var createTableSql = `create table if not exists ${tableName} (
      id integer IDENTITY(1,1),
      name varchar(100) not null,
      dob varchar(100) not null,
      primary key(id));`;
    
    var createParams = {
      ClusterIdentifier: 'redshift-cluster-1', /* required */
      Sql: createTableSql, /* required */
      Database: 'dev',
      DbUser: 'awsuser'
    };
    try {
      var createStateResult = await redshiftdata.executeStatement(createParams).promise();
    } catch (e) {
      console.log(e, e.stack);
    }
    
    /**
     * End Create Table Example
     **/
     
     
     /**
     * Start Insert Table Example
     **/
    var tableName = 'Employee';
    var insertSql = `INSERT INTO ${tableName} (name, dob) VALUES ('John Jonhson', 'HM');`;
    
    var insertParams = {
      ClusterIdentifier: 'redshift-cluster-1', /* required */
      Sql: insertSql, /* required */
      Database: 'dev',
      DbUser: 'awsuser'
    };
    
    try {
      var insertStateResult = await redshiftdata.executeStatement(insertParams).promise();
    } catch (e) {
      console.log(e, e.stack);
    }
    
    /**
     * End Insert Table Example
     **/
     
     /**
      * Start Query Status
      **/ 
    var params = {
      Id: insertStateResult.Id /* required */
    };
    redshiftdata.describeStatement(params, function(err, data) {
      if (err) console.log(err, err.stack); // an error occurred
      else     console.log(data);           // successful response
    });
    /**
      * End Query Status
    **/
    
    /**
     * Start Get All Data Example
     **/
    var tableName = 'Employee';
    var selectSql = `SELECT * FROM ${tableName}`;
    
    var selectParams = {
      ClusterIdentifier: 'redshift-cluster-1', /* required */
      Sql: selectSql, /* required */
      Database: 'dev',
      DbUser: 'awsuser'
    };
    
    try {
      var selectStateResult = await redshiftdata.executeStatement(selectParams).promise();
    } catch (e) {
      console.log(e, e.stack);
    }
    
    var params = {
      Id: selectStateResult.Id /* required */
    };
    
    let d = await checkStatusAndResult(params);
    console.log(d);
    /**
     * End Get All Data Example
     **/
  
    
    /**
     * Start Update Example
     **/
    var tableName = 'Employee';
    var updateSql = `UPDATE ${tableName} SET name='admin_${Math.floor(Math.random()*10000)}' WHERE id=1`;
    
    var updateParams = {
      ClusterIdentifier: 'redshift-cluster-1', /* required */
      Sql: updateSql, /* required */
      Database: 'dev',
      DbUser: 'awsuser'
    };
    
    try {
      var updateStateResult = await redshiftdata.executeStatement(updateParams).promise();
    } catch (e) {
      console.log(e, e.stack);
    }
    
    /**
     * End Update Example
     **/
    
};
