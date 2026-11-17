/* ЛР5: штатные REST API локального DataLens. Запуск через jobs/lr5.py.
 * Контракты: datalens-tech/datalens-{auth,us,backend,ui}; никаких SQL-правок БД BI.
 * Пароль поступает через stdin; токены остаются только в памяти процесса.
 */
const fs = require('node:fs');

async function main(input, fetchImpl=fetch) {
    let authorization;
    for (const service of ['bi-auth','bi-us','bi-control-api','bi-data-api']) {
        let ready=false;
        for(let attempt=0;attempt<90;attempt++) {
            try {
                const response=await fetchImpl(`http://${service}:8080/ping`,{signal:AbortSignal.timeout(3000)});
                if(response.status<500){ready=true;break;}
            } catch {}
            await new Promise(resolve=>setTimeout(resolve,2000));
        }
        if(!ready)throw new Error(`DataLens не готов: ${service}`);
    }
    async function call(service, path, body, method=body ? 'POST' : 'GET', extra={}) {
        const response = await fetchImpl(`http://${service}:8080${path}`, {
            method, headers: {'Content-Type':'application/json', ...(authorization ? {Authorization:authorization} : {}), ...extra},
            ...(body ? {body:JSON.stringify(body)} : {}), signal:AbortSignal.timeout(180000),
        });
        if (!response.ok) {
            let detail=await response.text();
            for (const secret of [authorization,authorization?.slice(7),input.adminPassword,input.env.WAREHOUSE_PASSWORD]) {
                if(secret)detail=detail.split(secret).join('[hidden]');
            }
            throw new Error(`${service} ${path}: HTTP ${response.status}\n${detail.slice(0,2000)}`);
        }
        return response;
    }
    const login = await call('bi-auth','/signin',{login:'admin',password:input.adminPassword});
    const cookies = login.headers.getSetCookie();
    let token;
    for (const cookie of cookies) {
        const value = cookie.slice(cookie.indexOf('=')+1).split(';')[0];
        try {
            const parsed=JSON.parse(decodeURIComponent(value));
            if (parsed.accessToken) token=parsed.accessToken;
        } catch {}
    }
    if (!token) throw new Error('В ответе signin нет accessToken');
    authorization='Bearer '+token;
    const request=async (...args)=>(await call(...args)).json();
    const list=await request('bi-us','/v2/workbooks?pageSize=100');
    const books=list.workbooks || list.entries || (Array.isArray(list) ? list : []);
    const title='ЛР5 - MOEX TQBR';
    const existing=books.filter(w=>w.title===title);
    if (existing.length>1) throw new Error('Найдено несколько книг ЛР5: оставь одну или укажи сохранённую data/lr5/dashboard.json');
    const workbookId=input.existing?.workbookId || existing[0]?.workbookId ||
        (await request('bi-us','/v2/workbooks',{title,description:'ЛР5: очищенные снимки MOEX; часовой пояс UTC; события — не сделки.'})).workbookId;
    if (!workbookId) throw new Error('API не вернул workbookId');
    const content=await request('bi-us',`/v2/workbooks/${workbookId}/entries?pageSize=100`);
    const entries=(content.entries || []).map(e=>({...e,name:e.name ?? (e.displayKey ?? e.key ?? '').split('/').pop()}));
    let connectionId=entries.find(e=>e.scope==='connection' && e.name==='LR5 ClickHouse')?.entryId;
    if (!connectionId) {
        const connection=await request('bi-control-api','/api/v1/connections',{
            name:'LR5 ClickHouse',type:'clickhouse',workbook_id:workbookId,db_name:'mart',
            host:'warehouse',port:8123,username:input.env.WAREHOUSE_USER || 'student',
            password:input.env.WAREHOUSE_PASSWORD || 'student',secure:'off',raw_sql_level:'dashsql',
        });
        connectionId=connection.id;
    }
    if (!connectionId) throw new Error('API не вернул connection.id');
    const definitions=[
      {name:'LR5 - События по часам суток (UTC)',x:'hour_utc',type:'integer',y:'events',
       query:'SELECT toInt64(hour_utc) AS hour_utc,toFloat64(events) AS events FROM mart.dashboard_hourly ORDER BY hour_utc',
       question:'В какие часы суток UTC поступает больше событий за всю историю?',why:'До 24 столбцов сравнивают часы суток, суммируя все даты; это не хронологическая линия. Пропуски не соединяются.'},
      {name:'LR5 - Топ-10 бумаг по событиям',x:'SECID',type:'string',y:'events',
       query:'SELECT SECID,toFloat64(events) AS events FROM mart.dashboard_top ORDER BY events DESC,SECID',
       question:'По каким бумагам больше всего событий за накопленную историю?',why:'Ранжированные столбцы сравнивают 10 подписанных бумаг одним цветом; цвет не кодирует 10 категорий.'},
      {name:'LR5 - Бумаг с событиями за сутки (UTC)',x:'event_date',type:'string',y:'securities',
       query:'SELECT toString(event_date) AS event_date,toFloat64(securities) AS securities FROM mart.dashboard_daily ORDER BY event_date',
       question:'Как меняется число различных бумаг, по которым получены события за сутки UTC?',why:'Суточные столбцы сравнивают покрытие источника; это не сумма distinct по часам.'},
    ];
    const charts=[];
    for (const def of definitions) {
        // Check every actual chart query through the same DataLens backend.
        const data=await request('bi-data-api',`/api/data/v1/connections/${connectionId}/typed_query`,{
            query_type:'generic_query',query_content:{query:def.query},parameters:[],
        },'POST',{'x-dl-workbookid':workbookId});
        const rows=data.data?.rows;
        if (!Array.isArray(rows) || !rows.length) throw new Error(`Пустой результат DataLens: ${def.name}`);
        const shared=JSON.parse(JSON.stringify(input.template));
        shared.connection={entryId:connectionId,type:'clickhouse',dataExportForbidden:false};
        shared.queryValue=def.query;
        shared.extraSettings={titleMode:'show',title:def.name};
        for (const [axis,name,type] of [['x',def.x,def.type],['y',def.y,'float']]) {
            const placeholder=shared.visualization.placeholders.find(p=>p.id===axis);
            placeholder.items=[{guid:name,title:name,datasetId:'ql-mocked-dataset',data_type:type,
                cast:type,type:'DIMENSION',calc_mode:'direct',inspectHidden:true,formulaHidden:true,noEdit:true}];
            placeholder.settings={...(placeholder.settings||{}),title:'on',titleValue:axis==='x'?def.x:def.y};
        }
        let entry=entries.find(e=>e.name===def.name && e.scope==='widget');
        if (!entry) entry=await request('bi-us','/v1/entries',{
            scope:'widget',type:'d3_ql_node',name:def.name,workbookId,mode:'publish',
            data:{shared:JSON.stringify(shared)},links:{connection:connectionId},
            annotation:{description:def.question+' '+def.why},
        });
        const saved=await request('bi-us',`/v1/entries/${entry.entryId}`);
        const config=JSON.parse(saved.data.shared);
        if (config.queryValue!==def.query) throw new Error(`Сохранённый чарт изменён: ${def.name}`);
        charts.push({id:entry.entryId,name:def.name,question:def.question,type:'column',why:def.why,rows:rows.length});
    }
    const items=charts.map((chart,i)=>({id:`c${i}`,type:'widget',namespace:'default',data:{hideTitle:false,
        tabs:[{id:`t${i}`,title:chart.name,chartId:chart.id,isDefault:true,params:{},autoHeight:false,description:chart.question}]}}));
    const dash={salt:'lr5-moex',schemeVersion:7,counter:4,
        tabs:[{id:'main',title:'MOEX TQBR - UTC',items,
          layout:charts.map((_,i)=>({i:`c${i}`,x:0,y:i*14,w:36,h:14})),aliases:{},connections:[]}],
        settings:{hideTabs:true,hideDashTitle:false,silentLoading:false,autoupdateInterval:null,dependentSelectors:true}};
    let dashboard=entries.find(e=>e.scope==='dash' && e.name==='ЛР5 - MOEX TQBR - UTC');
    if (!dashboard) dashboard=await request('bi-us','/v1/entries',{
        scope:'dash',type:'',name:'ЛР5 - MOEX TQBR - UTC',workbookId,mode:'publish',data:dash,
        links:Object.fromEntries(charts.map((c,i)=>[`chart${i}`,c.id])),
        annotation:{description:'Три вопроса к истории MOEX. Событие — снимок бумаги, не сделка и не оборот. Все даты UTC. Отсутствие событий не равно отсутствию торгов.'},
    });
    const saved=await request('bi-us',`/v1/entries/${dashboard.entryId}`);
    if (saved.data.tabs[0].items.length!==3) throw new Error('В дашборде не три графика');
    return {workbookId,connectionId,dashboardId:dashboard.entryId,charts,backend_queries_verified:true,
        saved_entries_verified:true,visual_review_required:true,checked_at_utc:new Date().toISOString()};
}

module.exports={main};
if (require.main===module || !module.parent) {
    main(JSON.parse(fs.readFileSync(0,'utf8'))).then(result=>process.stdout.write(JSON.stringify(result)))
      .catch(error=>{process.stderr.write(error.message+'\n');process.exitCode=1;});
}

