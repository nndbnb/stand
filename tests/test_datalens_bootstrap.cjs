const assert=require('node:assert/strict');
const {main}=require('../jobs/datalens_bootstrap.cjs');
const template=require('../conf/datalens/ql_column.json');
const validName=/^[\wА-Яа-яЁё_@()%]([\wА-Яа-яЁё_@().,:;'\u00A0\u180E\u2000-\u200B\u202F\u205F\u3000\uFEFF| \-–—−$*&%]*[\wА-Яа-яЁё_@()%]+)?$/;

async function test() {
    const entries=[];
    let workbook,creates=0,queries=0;
    async function fakeFetch(url,options) {
        const path=new URL(url).pathname;
        const body=options.body?JSON.parse(options.body):null;
        let data;
        const response=value=>({ok:true,status:200,json:async()=>value});
        if(path==='/ping')return response({});
        if(path==='/signin') {
            assert.equal(body.login,'admin');
            return {...response({}),headers:{getSetCookie:()=>['dl_auth='+encodeURIComponent(JSON.stringify({accessToken:'fixture-token'}))+'; Path=/']}};
        }
        assert.equal(options.headers.Authorization,'Bearer fixture-token');
        if(path==='/v2/workbooks' && options.method==='GET')data={workbooks:workbook?[workbook]:[]};
        else if(path==='/v2/workbooks') {assert.match(body.title,validName);workbook={workbookId:'book-fixture',title:body.title};data=workbook;}
        else if(path.endsWith('/entries')&&options.method==='GET')data={entries:entries.map(({name,...entry})=>({...entry,key:name,displayKey:name}))};
        else if(path==='/api/v1/connections') {
            assert.equal(Object.hasOwn(body,'ssl_ca_verify'),false);assert.equal(body.host,'warehouse');assert.equal(body.secure,'off');assert.equal(body.workbook_id,workbook.workbookId);
            entries.push({scope:'connection',name:body.name,entryId:'conn-fixture'});data={id:'conn-fixture'};
        } else if(path.endsWith('/typed_query')) {
            if(body.query_content.query.includes('mart.dashboard_daily'))assert.ok(body.query_content.query.startsWith('SELECT toString(event_date) AS event_date,'));
            assert.equal(body.query_type,'generic_query');assert.ok(body.query_content.query.startsWith('SELECT'));
            assert.equal(options.headers['x-dl-workbookid'],workbook.workbookId);queries++;
            data={data:{rows:[[10,123]],headers:[]}};
        } else if(path==='/v1/entries') {
            assert.match(body.name,validName);
            if(body.scope==='widget') {const config=JSON.parse(body.data.shared);if(config.queryValue.includes('mart.dashboard_daily'))assert.equal(config.visualization.placeholders.find(p=>p.id==='x').items[0].data_type,'string');}
            const entry={...body,entryId:'entry-'+(++creates)};entries.push(entry);data=entry;
        } else if(path.startsWith('/v1/entries/'))data=entries.find(e=>e.entryId===path.split('/').pop());
        else throw new Error('Unexpected path '+path);
        return response(data);
    }
    const input={adminPassword:'fixture-password',env:{},template};
    const first=await main(input,fakeFetch);
    assert.equal(first.charts.length,3);
    assert.equal(first.backend_queries_verified,true);
    assert.equal(first.visual_review_required,true);
    assert.equal(creates,4);
    const second=await main({...input,existing:first},fakeFetch);
    assert.equal(second.dashboardId,first.dashboardId);
    assert.equal(creates,4,'Repeated bootstrap must not duplicate charts/dashboard');
    assert.equal(queries,6,'Verify real query route on every run');
    assert.ok(!JSON.stringify(first).includes('fixture-token'));
    assert.ok(!JSON.stringify(first).includes('fixture-password'));
    console.log('PASS: native DataLens API payloads, authentication, three query routes, repeat bootstrap, no token in report');
}
test().catch(error=>{console.error(error);process.exitCode=1;});

