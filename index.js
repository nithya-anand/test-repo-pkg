// Global Labels are used for data_processing in biqquery dataobjects

const gbl_getLabels = {
  
      "portfolio": "addg",
      "product_group": "dataverse",
      "product": "dataverse",
      "env":  dataform.projectConfig.vars.dataverse_project.split("-").pop(),
      "dv_name":  "dataverse",
      "dps_name":  "dataverse_dataprocess"
};

// Calculate the hash for the dimensional key `dk` and hash_dif.
function fn_calculateHash(input_value) {
    const {
        glb_null_replace,
        glb_hash_concat,
        glb_hash_algorithm
    } = dv_env_vars;

    //const aliasPrefix = alias ? `${alias}.` : '';

    const fetchValue = input_value;
    const value = fetchValue.map((v) => `trim(coalesce(cast(${v} as STRING), '${glb_null_replace}'))`).join('||"~"||');
    const calculateHash = `TO_HEX(${glb_hash_algorithm}(${value}))`
    return `(${calculateHash})`;
}


// Calculate the business key ,'bk'. '<>' for readability
function fn_calculateConcat(input_value) {
    const {
        glb_null_replace
    } = dv_env_vars;
    const fetchValue = input_value;
    const value = fetchValue.map((v) => `coalesce(cast(${v} as STRING), '${glb_null_replace}')`).join('||"<>"||');
    const calculateHash = `(${value})`
    return `(${calculateHash})`;
}

function fn_getNullDate() {
    return `cast(null as date)`;
}

function fn_getNullDateTime() {
    return `cast(null as datetime)`;
}

// Retrieve the hash key for the column `dk`.
function fn_getHashKey(input_value) {
    const fetchValue = input_value;
    return `${fetchValue[0]}`;
}


// Generate UUID for surrogate key
function fn_getUniqueKey() {
    const fetchValue = 'GENERATE_UUID()';
    return `${fetchValue}`;
}

function fn_getTenantCd(source, description, code) {
    //import the source code from source variable
    const {
        source_code
    } = source;
    // return the concat statement
    return `CONCAT('${source_code}','^^','DESC:',upper(${description}),'^^','CODE:',${code})`;
}

//To generate Case statment for ccl enrichment
function fn_ccl_enrich(ccl_code, alias) {
    //get ccl_code to lower case
    let v_ccl_code = ccl_code.toLowerCase();

    //import the file object to fetch necessary details
    const cerner_ccl_enrichment = require("definitions/param/source/cerner_ccl_enrichment.js");

    //create the empty variables and aliasPrefix to get alias value if available
    let ccl_query = "";
    let default_query = "";
    const aliasPrefix = alias ? `${alias}.` : '';

    // switch case to create pass the proper parameter and create statement
    switch (v_ccl_code) {
        case "admit_src_cd":
            ccl_query = Object.entries(cerner_ccl_enrichment.admit_src_cd).map(function([key, value]) {
                if (key != "default") {
                    return `when cast(${aliasPrefix}${v_ccl_code} as string) = "${key}" then "${value}"`;
                }
            }).join('\n');
            default_query = Object.entries(cerner_ccl_enrichment.admit_src_cd).map(function([key, value]) {
                if (key == "default") {
                    return `else "${value}"`;
                }
            }).join('\n');
            break;

        case "disch_disposition_cd":
            ccl_query = Object.entries(cerner_ccl_enrichment.disch_disposition_cd).map(function([key, value]) {
                if (key != "default") {
                    return `when cast(${aliasPrefix}${v_ccl_code} as string) = "${key}" then "${value}"`;
                }
            }).join('\n');
            default_query = Object.entries(cerner_ccl_enrichment.disch_disposition_cd).map(function([key, value]) {
                if (key == "default") {
                    return `else "${value}"`;
                }
            }).join('\n');
            break;
    }
    //trimming the unncessary spaces before default statement
    default_query = default_query.trim();
    return ` case ${ccl_query} ${default_query} end`;

}


function fn_setHistWhere(object, alias) {
    //extract column name used for historical load and the value
    const hist_src_col = object.source_hist_load_column;
    const hist_load_ts = dv_env_vars.glb_hist_load_ts;

    // alias if passed will be used else skipped
    const aliasPrefix = alias ? `${alias}.` : '';

    return `${aliasPrefix}${hist_src_col} > '${hist_load_ts}'`;
}


function fn_setIncrWhere(source, batch, alias) {
    // extract values from object being passed
    let inc_src_col = source.source_incr_load_column;
    const fullScanFlag = source.source_full_read_flag;
    const OverlapFlag = source.pvt_overlap_days;

    const batch_code = batch.batch_code;

    //extract environmental and global variables
    const batch_table = dv_env_vars.glb_dv_audit_batch_control;
    const hist_load_ts = dv_env_vars.glb_hist_load_ts;

    //declare individual query condition
    const greaterThan = `(SELECT batch_extract_load_start_ts FROM ${batch_table} WHERE batch_code = "${batch_code}")`;
    const lessThan = `(SELECT batch_extract_load_end_ts FROM ${batch_table} WHERE batch_code = "${batch_code}")`;

    // alias if passed will be used else skipped
    const aliasPrefix = alias ? `${alias}.` : '';

    // consolidated condition
    const condition = fullScanFlag === "N" ?
        `BETWEEN ${greaterThan} AND ${lessThan}` :
        `> "${hist_load_ts}"`;

    const condition1 = fullScanFlag === "N" ?
        `BETWEEN DATETIME_SUB(${greaterThan},INTERVAL ${OverlapFlag} day)  AND ${lessThan}` :
        `> "${hist_load_ts}"`;
        // const condition2 = fullScanFlag === "N" ?
        // `BETWEEN DATETIME_SUB(${greaterThan},INTERVAL 7 day)  AND ${lessThan}` :
        // `> "${hist_load_ts}"`;
    
    if (OverlapFlag){
        return `WHERE cast(${aliasPrefix}${inc_src_col} as DATETIME) ${condition1}`;
    }
    else {
        return `WHERE cast(${aliasPrefix}${inc_src_col} as DATETIME) ${condition}`;
    }

}

function fn_updateColList(column_list) {
    return column_list.map(col => `tgt.${col}=src.${col}`).join(',');
}


//----UPDATED (replaced 'AND' with 'OR')----- fetch 'compare_list' array and create comparison statement separated by logical 'OR'
function fn_updateColCompareList(column_list) {
    return column_list.map(col => `coalesce(CAST(tgt.${col} AS STRING),'^') != coalesce(CAST(src.${col} AS STRING),'^')`).join(' OR ');
}


// fn_SCD1load takes processed table, target table, and target object as arguments.
// It returns the SCD Type-1 statement by comparing necessary values between the processed and target tables.
function fn_SCD1load(processed_table, target_table, target) {
    // Destructuring properties from the target object
    const {
        bk,
        dk,
        hk,
        insert_list,
        update_list,
        compare_list
    } = target;

    // Generate the list of columns to update
    const updt_list = fn_updateColList(update_list);

    // Generate the list of columns to compare
    const cmp_list = fn_updateColCompareList(compare_list);

    // Common part of the MERGE statement
    const baseQuery = `
    MERGE ${target_table} AS tgt
    USING (
      SELECT *, RANK() OVER (PARTITION BY ${bk} ORDER BY ${dk} DESC) AS rnk
      FROM ${processed_table}
    ) AS src
    ON src.${bk} = tgt.${bk}`;

    // Constructing the complete MERGE statement based on the presence of a hash difference (hk)
    if (hk === "") {
        return `
      ${baseQuery}
      WHEN MATCHED AND ${cmp_list}
      THEN UPDATE SET ${updt_list}
      WHEN NOT MATCHED
      THEN INSERT (${insert_list}) VALUES (${insert_list})`;
    } else {
        return `
      ${baseQuery}
      WHEN MATCHED AND src.${hk} != tgt.${hk}
      THEN UPDATE SET ${updt_list}
      WHEN NOT MATCHED
      THEN INSERT (${insert_list}) VALUES (${insert_list})`;
    }
}

/*UPDATED LOGIC*/
// fn_scd2_load takes processed table, target table, and target object as arguments.
// It returns the SCD Type-2 statement by comparing necessary values between the processed and target tables.

function fn_SCD2load(processed_table, target_table, target, load_type) {
    // Destructuring properties from the target object
    const {
        bk,
        hash_dif,
        dk,
        insert_list,
        compare_list
    } = target;

    // Initialize variables for unique_key (bk), hash difference (hk), datakey (dk),
    // column list for insert, and compare list using the provided functions.
    let unique_key = bk;
    //let hash_dif = hk;
    let datakey = dk;
    let column_list = insert_list;

    // Generate the list of columns to compare
    let compare_listString = fn_updateColCompareList(compare_list);
    // Checking if hash_dif exists or not for the given target table, ignore hash key compariso if not exists
    if (hash_dif === "") {
        return `
      MERGE INTO ${target_table} AS tgt
      USING (
        -- Selecting all the records from the processed table, duplicating unique with different alias name
        SELECT distinct ${processed_table}.${unique_key} AS join_key, ${processed_table}.*
        FROM ${processed_table}
        WHERE vld_fm_ts IN (
          SELECT vld_fm_ts
          FROM (
            SELECT vld_fm_ts, ROW_NUMBER() OVER (PARTITION BY ${unique_key} ORDER BY vld_fm_ts DESC) AS rank
            FROM ${processed_table}
          )
          WHERE rank = 1
        )
        UNION ALL
        -- Adding a Null record for the inserting condition
        SELECT distinct CAST(NULL AS String) as join_key, src.*
        FROM ${processed_table} src
        JOIN ${target_table} tgt
        ON src.${unique_key} = tgt.${unique_key}
        WHERE (${compare_listString}
          AND tgt.vld_to_ts = '9999-12-31T00:00:00')
          AND src.vld_fm_ts IN (
            SELECT vld_fm_ts
            FROM (
              SELECT vld_fm_ts, ROW_NUMBER() OVER (PARTITION BY ${unique_key} ORDER BY vld_fm_ts DESC) AS rank
              FROM ${processed_table}
            )
            WHERE rank = 1
          )
      ) src
      -- Joining on join key to create duplicates of records to be updated or inserted
      ON src.join_key = tgt.${unique_key}
      WHEN MATCHED AND ${compare_listString} AND tgt.vld_to_ts = '9999-12-31T00:00:00'
      THEN
        -- Update condition for updating valid_to_ts value only and any data column
        UPDATE SET tgt.vld_to_ts = src.vld_fm_ts
      -- Insert records whether new or updated
      WHEN NOT MATCHED AND src.vld_to_ts='9999-12-31T00:00:00' THEN 
        INSERT (${column_list})
        VALUES (${column_list})`;
    } else {
        // Include hash_dif comparison if hash_dif exists for given target table
        return `
      MERGE INTO ${target_table} AS tgt 
      USING (
        -- Selecting all the records from the target table, duplicating unique with different alias name
        SELECT distinct ${processed_table}.${unique_key} AS join_key, ${processed_table}.*
        FROM ${processed_table}
        WHERE vld_fm_ts IN (
          SELECT vld_fm_ts
          FROM (
            SELECT vld_fm_ts, ROW_NUMBER() OVER (PARTITION BY ${unique_key} ORDER BY vld_fm_ts DESC) AS rank
            FROM ${processed_table}
          )
          WHERE rank = 1
        )
        UNION ALL
        -- Adding a Null record for the inserting condition
        SELECT distinct CAST(NULL AS String) as join_key, src.*
        FROM ${processed_table} src
        JOIN ${target_table} tgt
        ON src.${unique_key} = tgt.${unique_key}
        WHERE (
          src.${hash_dif} != tgt.${hash_dif} AND
          --src.${datakey} != tgt.${datakey} AND
          tgt.vld_to_ts = '9999-12-31T00:00:00'
        )
        AND src.vld_fm_ts IN (
          SELECT vld_fm_ts
          FROM (
            SELECT vld_fm_ts, ROW_NUMBER() OVER (PARTITION BY ${unique_key} ORDER BY vld_fm_ts DESC) AS rank
            FROM ${processed_table}
          )
          WHERE rank = 1
        )
      ) src
      -- Joining on join key to create duplicates of records to be updated or inserted
      ON src.join_key = tgt.${unique_key}
      WHEN MATCHED AND src.${hash_dif} != tgt.${hash_dif} AND tgt.vld_to_ts = '9999-12-31T00:00:00'
      THEN
        -- Update condition for updating valid_to_ts value only and any data column
        UPDATE SET tgt.vld_to_ts = src.vld_fm_ts
      -- Insert records whether new or updated
      WHEN NOT MATCHED THEN
        INSERT (${column_list})
        VALUES (${column_list})`;
    }
}

/*UPDATED LOGIC*/
//fn_insertload takes processed table name, target table name and object as the arguments
// The function only inserts the data after comparing necessary attributes, no updates are done on data
function fn_insertload(processed_table, target_table, target) {
    // Destructuring properties from the target object

    const {
        bk,
        hash_dif,
        dk,
        insert_list,
        compare_list
    } = target;
    
    const {
        glb_null_replace,
        glb_hash_concat,
        glb_hash_algorithm
    } = dv_env_vars;

    // Initialize variables for unique_key (bk), hash difference (hk), datakey (dk),
    // column list for insert, and compare list using the provided functions.
    let unique_key = bk;
    let unique_key1 = dk;
    let datakey = dk;
    let column_list = insert_list;

    let compare_listString = fn_updateColCompareList(compare_list);

	  let tgt_join = unique_key.map((v) => `tgt.${v} `).join(' || ');
    let src_join_key = unique_key.map((v) => `${processed_table}.${v} `).join(' || ');
    let rank_value='=1';

    let find_index=target_table.indexOf('_arr')
    if (find_index>1) {
	
   rank_value= '=rank';
   tgt_join = unique_key1.map((v) => `tgt.${v} `).join(' || ');
   src_join_key = unique_key1.map((v) => `${processed_table}.${v} `).join(' || ');

}
    return `
      MERGE INTO ${target_table} AS tgt
      USING (
        -- Selecting all the records from the processed table, duplicating unique with different alias name
        SELECT ${src_join_key} AS join_key,
         ${processed_table}.*
        FROM ${processed_table}
        WHERE vld_fm_ts IN (
          SELECT vld_fm_ts
          FROM (
            SELECT vld_fm_ts, ROW_NUMBER() OVER (PARTITION BY ${src_join_key} ORDER BY vld_fm_ts DESC) AS rank
            FROM ${processed_table}
          )
          WHERE rank ${rank_value}
        )
      ) src
      -- Joining on join key to create duplicates of records to be updated or inserted
      ON src.join_key = ${tgt_join} AND NOT(${compare_listString})
      --ON src.join_key = tgt.${unique_key}.replace('||','|| tgt.') AND NOT(${compare_listString})
      -- Insert records whether new or updated
      WHEN NOT MATCHED THEN
        INSERT (${column_list})
        VALUES (${column_list});`;
};

/** Exception Handling functions **/

function fn_exceptionFind(exception, source) {

    //update naming and import style
    let expt_tgt_id = exception.expt_tgt_id;
    let expt_tgt_table = source.target_table;
    let expt_src_table = source.source_table;
    let expt_src_cd = exception.src_cd;
    let expt_src_bk = exception.tgt_expt_bk;
    let expt_src_bk_value = exception.tgt_expt_bk;
    let expt_vld_fm_ts = 'vld_fm_ts'; //vld_fm_ts from process tbl
    let expt_vld_to_ts = '9999-12-31T00:00:00';

    let expt_tgt_dk = exception.tgt_expt_dk;

    expt_tgt_dk = expt_tgt_dk.map(function(element, index) {
        return `'${element}' AS expt_tgt_dk_${index},
    ${element} AS expt_tgt_dk_value_${index}`
    });

    let case_statement = exception.tgt_expt_dk;
    case_statement = case_statement.map(function(element, index) {
        return ` CASE 
    WHEN ${element} = '-1' THEN '9001'
    ELSE cast(NULL as string)
    END AS expt_cd_${index}`
    }).join(',\n');

    return `${expt_tgt_id} AS expt_tgt_id,
    '${expt_tgt_table}' AS expt_tgt_table,
    '${expt_src_table}' AS expt_src_table,
    '${expt_src_cd}' AS expt_src_cd,
    '${expt_src_bk}' AS expt_src_bk,
     ${expt_src_bk_value} AS expt_src_bk_value,
     ${expt_tgt_dk},
     ${case_statement},
     ${expt_vld_fm_ts} AS expt_vld_fm_ts,
    cast('${expt_vld_to_ts}' as datetime) AS expt_vld_to_ts`;
}

function fn_generateExceptionSQL(exception,source, stage_table) {
  const {
    expt_tgt_id,
    src_cd,
    tgt_expt_dk,
    src_expt_bk,
  } = exception;

  const {
    source_table,
    target_table
  } = source;

  const sqlBlocks = tgt_expt_dk.map((dk, index) => {
    const sql = `
      select distinct
        cast(expt_tgt_id as Integer) as expt_tgt_id,
        cast(expt_tgt_table as string) as tgt_table,
        cast(expt_src_table as string) as src_table,
        cast(expt_src_cd as string) as src_cd,
        cast(expt_src_bk as string) as src_bk,
        cast(expt_src_bk_value as string) as src_bk_value,
        cast(expt_tgt_dk_${index} as string) as tgt_dk,
        cast(expt_tgt_dk_value_${index} as string) as tgt_dk_value,
        cast(expt_cd_${index} as string) as expt_cd,
        cast(expt_vld_fm_ts as datetime) as vld_fm_ts,
        cast(expt_vld_to_ts as datetime) as vld_to_ts,
      from ${stage_table}
      --where expt_cd_${index} IS NOT NULL
    `;
    return sql;
  });

  return sqlBlocks.join('\nUNION ALL\n');
}

// fn_exceptionLoad takes stage table, exception table, exception and source object as arguments.
// It returns the SCD Type-1 statement by comparing necessary values between the processed and target tables.
function fn_exceptionLoad(stage_table, exception_table, exception, source) {
   
    const baseQuery = `
    MERGE INTO ${exception_table} AS tgt
    USING (
      ${fn_generateExceptionSQL(exception, source, stage_table)}
    ) AS src
    ON src.src_bk_value = tgt.src_bk_value and src.tgt_dk = tgt.tgt_dk 
    WHEN MATCHED and (src.tgt_dk_value != tgt.tgt_dk_value) and tgt.expt_tgt_id = src.expt_tgt_id
    THEN UPDATE SET tgt.vld_to_ts = src.vld_fm_ts
    WHEN NOT MATCHED and src.expt_cd IS NOT NULL
    THEN INSERT(expt_tgt_id,tgt_table,src_table,src_cd,src_bk,src_bk_value,
    tgt_dk,tgt_dk_value,expt_cd,vld_fm_ts,vld_to_ts) 
    VALUES (expt_tgt_id,tgt_table,src_table,src_cd,src_bk,src_bk_value,
    tgt_dk,tgt_dk_value,expt_cd,vld_fm_ts,vld_to_ts)`;

    return baseQuery;
}

function fn_getExceptionDK(src_col,tgt_col, dk_col){
  return `
CASE
WHEN (${src_col} IS NULL or cast(${src_col} as string) = '0') THEN '-2'
WHEN (${tgt_col} IS NULL or cast(${tgt_col} as string) = '0') and (${src_col} IS NOT NULL and cast(${src_col} as string) != '0') THEN '-1'
WHEN (${tgt_col} IS NOT NULL or cast(${tgt_col} as string) != '0') and ${dk_col} IS NULL THEN '-1'
ELSE ${dk_col}
END`
}


function fn_defineArrayList(col_list) {
    return col_list.map((col) => `"${col}"`).join(',');
}

function fn_GetArrayVal(col_list) {
      col_list = Object.entries(col_list).map(([key, value]) => "'"+value+"'");
return col_list;
}

function fn_auditBatchStart(batch) {
    /*- **Function Description:** Used to initiate the auditing of a data batch and record the start of the audit.
      - **Input:** batch object (batch_code, audit_batch_id), user object (user_id, user_name).
      - **Output:** out_batch_run_info (String) indicating the success or failure of the batch audit initiation.*/
const {
        batch_source_id
    } = batch;
    let batch_code = batch.batch_code;
    let batch_desc = batch.batch_desc;
    let col_list = batch_source_id;
    const batch_source_id_list=fn_defineArrayList(col_list);
    let dataverse_project = dataform.projectConfig.vars.dataverse_project;
    let dataprocess_dataset = dataform.projectConfig.vars.stg_dataverse_process_dataset;
    let env=dataform.projectConfig.vars.dataverse_project.split("-").pop();
    //let env=genv.split("-").pop();
    return `DECLARE out_audit_batch_info STRING;
              SET @@query_label = "name:dataverse_process,portfolio:addg,product_group:dataverse,product:dataverse,env:${env}";  

    TRUNCATE TABLE \`${dataverse_project}.${dataprocess_dataset}.dv_audit_run_trace\`;
    CALL \`${dataverse_project}.${dataprocess_dataset}.auditBatchStart\`("${batch_code}",[${batch_source_id_list}],"${batch_desc}",out_audit_batch_info);
  
    INSERT INTO \`${dataverse_project}.${dataprocess_dataset}.dv_audit_run_trace\`
    (run_timestamp,note,output)
    VALUES(current_datetime(),'auditBatchStart',out_audit_batch_info);`;
}


function fn_auditJobStart(batch, job) {
    /*- **Function Description:** Initiates the auditing of a specific job within a batch and records the start of the job audit.
      - **Input:** batch object (batch_code), job object (job_code).
      - **Output:** out_job_run_info (String) indicating the success or failure of the job audit initiation.*/
    let batch_code = batch.batch_code;
    let job_code = job.job_code;
    let job_desc = job.job_desc;
    let batch_job_list = batch.job_code;
    let dataverse_project = dataform.projectConfig.vars.dataverse_project;
    let dataprocess_dataset = dataform.projectConfig.vars.stg_dataverse_process_dataset;
    let env=dataform.projectConfig.vars.dataverse_project.split("-").pop();
    if (batch_job_list.includes(job_code)) {
        // Job code  found in the list call the JobOpen
        
        return `DECLARE out_job_run_info STRING;
            SET @@query_label = "name:dataverse_process,portfolio:addg,product_group:dataverse,product:dataverse,env:${env}";

        CALL \`${dataverse_project}.${dataprocess_dataset}.auditJobStart\`("${batch_code}","${job_code}","${job_desc}",out_job_run_info);
            INSERT INTO \`${dataverse_project}.${dataprocess_dataset}.dv_audit_run_trace\`(run_timestamp,note,output) VALUES(current_datetime(),'auditJobStart',out_job_run_info);`;
    } else {
        // Job code not found in the list, handle the exception
        return ` INSERT INTO \`${dataverse_project}.${dataprocess_dataset}.dv_audit_run_trace\` VALUES(current_datetime(),'job not found','exception: not in the list');`;
    }
}


function fn_auditStepStart(batch, job, filename) {
    /*- **Function Description:** Initiates the auditing of a specific step within a batch and records the start of the step.
      - **Input:** batch object (batch_code, audit_batch_id), job object (job_code), filename (String).
      - **Output:** out_stp_run_info (String) indicating the success or failure of the step audit initiation.*/
    let batch_code = batch.batch_code;
    let job_code = job.job_code;
    let subject_area= job.lbl_subject_area;
    let source=job.lbl_source;
    let dataverse_project = dataform.projectConfig.vars.dataverse_project;
    let dataprocess_dataset = dataform.projectConfig.vars.stg_dataverse_process_dataset;
    let env=dataform.projectConfig.vars.dataverse_project.split("-").pop();
    return `DECLARE out_stp_run_info STRING;
            SET @@query_label = "name:dataverse_process,portfolio:addg,product_group:dataverse,product:dataverse,env:${env},subject_area:${subject_area},source:${source}";
            
    CALL \`${dataverse_project}.${dataprocess_dataset}.auditStepStart\`("${batch_code}","${job_code}","${filename}",out_stp_run_info);
    
    INSERT INTO \`${dataverse_project}.${dataprocess_dataset}.dv_audit_run_trace\`
    (run_timestamp,note,output)
    VALUES(current_datetime(),'auditStepStart',out_stp_run_info);`;
}


function fn_auditStepEnd(batch, job, filename) {
    /*- **Function Description:** Marks the end of an auditing step within a batch, records the audit closure, and logs relevant information.
      - **Input:** batch object (batch_code), job object (job_code), filename (String).
      - **Output:** out_stp_run_info (String) containing information about the step audit closure.*/
    let batch_code = batch.batch_code;
    let job_code = job.job_code;
    let dataverse_project = dataform.projectConfig.vars.dataverse_project;
    let dataprocess_dataset = dataform.projectConfig.vars.stg_dataverse_process_dataset;
	let env=dataform.projectConfig.vars.dataverse_project.split("-").pop();
    return `SET @@query_label = "name:dataverse_process,portfolio:addg,product_group:dataverse,product:dataverse,env:${env}";

    CALL \`${dataverse_project}.${dataprocess_dataset}.auditStepEnd\`("${batch_code}","${job_code}","${filename}",out_stp_run_info);
    
    
    INSERT INTO \`${dataverse_project}.${dataprocess_dataset}.dv_audit_run_trace\`
    (run_timestamp,note,output)
    VALUES(current_datetime(),'auditStepEnd',out_stp_run_info);`;
}


function fn_auditJobEnd(batch, job) {
    /* - **Function Description:** Marks the end of a job audit within a batch, records the audit closure, and logs relevant information.
       - **Input:** batch object (batch_code), job object (job_code).
       - **Output:** out_job_run_info (String) containing information about the job audit closure.*/
    let batch_code = batch.batch_code;
    let job_code = job.job_code;
    let dataverse_project = dataform.projectConfig.vars.dataverse_project;
    let dataprocess_dataset = dataform.projectConfig.vars.stg_dataverse_process_dataset;
	let env=dataform.projectConfig.vars.dataverse_project.split("-").pop();
    return `DECLARE out_job_run_info STRING;
            SET @@query_label = "name:dataverse_process,portfolio:addg,product_group:dataverse,product:dataverse,env:${env}";

    CALL \`${dataverse_project}.${dataprocess_dataset}.auditJobEnd\`("${batch_code}","${job_code}",out_job_run_info);
    
    INSERT INTO \`${dataverse_project}.${dataprocess_dataset}.dv_audit_run_trace\`
    (run_timestamp,note,output)
    VALUES(current_datetime(),'auditJobEnd',out_job_run_info);`;
}



function fn_auditBatchEnd(batch) {
    /*- **Function Description:** Marks the completion of batch auditing, records the audit closure, and logs relevant information.
      - **Input:** batch object (batch_code, audit_batch_id).
      - **Output:** out_batch_run_info (String) containing information about the batch audit closure.*/
    let batch_code = batch.batch_code;
    let batch_desc = batch.batch_desc;
    let job_code = batch.job_code;
    let dataverse_project = dataform.projectConfig.vars.dataverse_project;
    let dataprocess_dataset = dataform.projectConfig.vars.stg_dataverse_process_dataset;
	let env=dataform.projectConfig.vars.dataverse_project.split("-").pop();
	
    return `DECLARE out_audit_batch_info STRING;
            SET @@query_label = "name:dataverse_process,portfolio:addg,product_group:dataverse,product:dataverse,env:${env}";

    CALL \`${dataverse_project}.${dataprocess_dataset}.auditBatchEnd\`("${batch_code}","${job_code}","${batch_desc}",out_audit_batch_info);
    
    INSERT INTO \`${dataverse_project}.${dataprocess_dataset}.dv_audit_run_trace\`
    (run_timestamp,note,output)
    VALUES(current_datetime(),'auditBatchEnd',out_audit_batch_info);`;
}

function fn_getSourceCommonCode(source_common_code, alias) {
    const {
        source_key,
        source_value,
        descr_key,
        descr_value,
        code_key,
        code_value,
        concat_string,
        null_replace
    } = source_common_code;
    // alias if passed will be used else skipped
    const aliasPrefix = alias ? `${alias}.` : '';

    const descr = descr_value.map((descr_value) => `coalesce(trim(cast(${aliasPrefix}${descr_value} as STRING)), '${null_replace}')`);
    const code = code_value.map((code_value) => `coalesce(trim(cast(${aliasPrefix}${code_value} as STRING)), '${null_replace}')`);

    return `CONCAT('${source_key}','${concat_string}','${source_value}','${concat_string}','${descr_key}',${descr},'${concat_string}','${code_key}',${code})`;
}

function fn_getSourceCommonCode(source_common_code, alias) {
    const {
        source_key,
        source_value,
        descr_key,
        descr_value,
        code_key,
        code_value,
        concat_string,
        null_replace
    } = source_common_code;
    // alias if passed will be used else skipped
    const aliasPrefix = alias ? `${alias}.` : '';

    //const descr = descr_value.map((descr_value) => `upper(coalesce(trim(cast(${aliasPrefix}${descr_value} as STRING)), '${null_replace}'))`);
    const descr = descr_value.map((descr_value) => `coalesce(UPPER(NULLIF(TRIM(CAST(${aliasPrefix}${descr_value} AS STRING)), '')),'${null_replace}')`);
    //coalesce(UPPER(NULLIF(TRIM(CAST(${aliasPrefix}${descr_value} AS STRING)), '')),'${null_replace}')
    const code = code_value.map((code_value) => `coalesce(trim(cast(${aliasPrefix}${code_value} as STRING)), '${null_replace}')`);

    return `CONCAT('${source_key}','${concat_string}','${source_value}','${concat_string}','${descr_key}',${descr},'${concat_string}','${code_key}',${code})`;
}

function fn_getSK(src_cd, cmn_cd_sk){
  return `CASE
  WHEN ${src_cd} IS NULL THEN '-2'
  WHEN ${src_cd} IS NOT NULL and ${cmn_cd_sk} IS NULL THEN '-1'
  ELSE ${cmn_cd_sk}
  END`
}

function fn_getDefaultStr(value){
  let {glb_empty_replace} = dv_env_vars;
  return ` coalesce(cast(${value} as string),'${glb_empty_replace}')`;
}

module.exports = {
    fn_calculateHash,
    fn_calculateConcat,
    fn_getHashKey,
    fn_getUniqueKey,
    fn_getNullDate,
    fn_getNullDateTime,
    fn_getTenantCd,
    fn_ccl_enrich,
    fn_setHistWhere,
    fn_setIncrWhere,
    fn_SCD1load,
    fn_SCD2load,
    fn_insertload,
    fn_exceptionFind,
    fn_getExceptionDK,
    fn_generateExceptionSQL,
    fn_exceptionLoad,
    fn_auditBatchStart,
    fn_auditJobStart,
    fn_auditStepStart,
    fn_auditStepEnd,
    fn_auditJobEnd,
    fn_auditBatchEnd,
    fn_getSourceCommonCode,
    fn_defineArrayList,
    fn_GetArrayVal,
    fn_updateColCompareList,
    fn_defineArrayList,
    fn_getSK,
    fn_getDefaultStr,
    gbl_getLabels
};