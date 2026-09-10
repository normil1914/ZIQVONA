const http=require('http');
const fs=require('fs');
const path=require('path');
const WebSocket=require('ws');

const PORT=process.env.PORT||3000;

const users=new Map();
const clients=new Map();
const profiles=new Map();
const messages=[];

const server=http.createServer((req,res)=>{
  let p=req.url.split('?')[0];

  if(p==='/') p='/index.html';

  const f=path.join(
    __dirname,
    'public',
    path.normalize(p).replace(/^\.\.[\\/]+/,'')
  );

  fs.readFile(f,(e,d)=>{
    if(e){
      res.writeHead(404);
      return res.end('Not found');
    }

    const types={
      '.html':'text/html',
      '.js':'text/javascript',
      '.css':'text/css'
    };

    res.writeHead(200,{
      'Content-Type':
        types[path.extname(f)] ||
        'application/octet-stream'
    });

    res.end(d);
  });
});

const wss=new WebSocket.Server({server});

function send(ws,o){
  if(ws && ws.readyState===1){
    ws.send(JSON.stringify(o));
  }
}

function presence(){
  return [...users.keys()];
}

function publicProfile(username){
  const p=profiles.get(username);

  return {
    username,
    displayName:p?.displayName || username,
    status:p?.status || 'Disponib',
    avatar:p?.avatar || ''
  };
}

function allProfiles(){
  return [...profiles.keys()].map(publicProfile);
}

wss.on('connection',ws=>{

  send(ws,{type:'welcome'});

  ws.on('message',raw=>{

    let m;

    try{
      m=JSON.parse(raw);
    }catch{
      return;
    }

    if(m.type==='login'){

      const u=String(
        m.username||''
      ).trim().slice(0,30);

      if(!u){
        return send(ws,{
          type:'error',
          message:'Mete yon non.'
        });
      }

      if(
        users.has(u) &&
        users.get(u)!==ws
      ){
        return send(ws,{
          type:'error',
          message:'Non sa deja konekte.'
        });
      }

      users.set(u,ws);
      clients.set(ws,u);

      if(!profiles.has(u)){
        profiles.set(u,{
          displayName:u,
          status:'Disponib',
          avatar:''
        });
      }

      send(ws,{
        type:'login_ok',
        username:u,
        online:presence(),
        profiles:allProfiles()
      });

      wss.clients.forEach(c=>{
        send(c,{
          type:'presence',
          online:presence(),
          profiles:allProfiles()
        });
      });

      return;
    }

    const from=clients.get(ws);

    if(m.type==='profile_get'){

      if(!from) return;

      send(ws,{
        type:'profile',
        profile:publicProfile(from)
      });

      return;
    }

    if(m.type==='profile_update'){

      if(!from) return;

      const current=
        profiles.get(from) || {};

      profiles.set(from,{
        displayName:
          String(
            m.displayName ??
            current.displayName ??
            from
          )
          .trim()
          .slice(0,50),

        status:
          String(
            m.status ??
            current.status ??
            'Disponib'
          )
          .trim()
          .slice(0,120),

        avatar:
          String(
            m.avatar ??
            current.avatar ??
            ''
          )
          .slice(0,100000)
      });

      send(ws,{
        type:'profile',
        profile:publicProfile(from)
      });

      wss.clients.forEach(c=>{
        send(c,{
          type:'profiles',
          profiles:allProfiles()
        });
      });

      return;
    }

    if(m.type==='search_users'){

      if(!from) return;

      const q=String(
        m.query||''
      ).trim().toLowerCase();

      const results=
        allProfiles()
        .filter(p=>p.username!==from)
        .filter(p=>{
          if(!q) return true;

          return (
            p.username.toLowerCase().includes(q) ||
            p.displayName.toLowerCase().includes(q)
          );
        })
        .map(p=>({
          ...p,
          online:users.has(p.username)
        }))
        .slice(0,50);

      send(ws,{
        type:'search_results',
        results
      });

      return;
    }

    if(m.type==='message'){

      const to=String(
        m.to||''
      );

      const text=String(
        m.text||''
      )
      .trim()
      .slice(0,2000);

      if(!from||!to||!text) return;

      const packet={
        type:'message',
        id:
          Date.now()+
          '-' +
          Math.random(),

        from,
        to,
        text,
        time:new Date().toISOString()
      };

      messages.push(packet);

      send(ws,packet);
      send(users.get(to),packet);

      return;
    }

    if(m.type==='history'){

      if(!from) return;

      const withUser=
        String(m.with||'');

      const history=
        messages.filter(x=>
          (
            x.from===from &&
            x.to===withUser
          ) ||
          (
            x.from===withUser &&
            x.to===from
          )
        );

      send(ws,{
        type:'history',
        with:withUser,
        messages:history
      });

      return;
    }

    if(m.type==='typing'){

      const to=String(m.to||'');

      if(
        from &&
        users.has(to)
      ){
        send(
          users.get(to),
          {
            type:'typing',
            from
          }
        );
      }

      return;
    }

    if(
      m.type==='call-offer' ||
      m.type==='call-answer' ||
      m.type==='ice' ||
      m.type==='call-end'
    ){

      const to=String(m.to||'');

      if(
        from &&
        users.has(to)
      ){
        send(
          users.get(to),
          {
            ...m,
            from
          }
        );
      }

      return;
    }

  });

  ws.on('close',()=>{

    const u=clients.get(ws);

    if(u){

      users.delete(u);
      clients.delete(ws);

      wss.clients.forEach(c=>{
        send(c,{
          type:'presence',
          online:presence(),
          profiles:allProfiles()
        });
      });
    }
  });

});

server.listen(
  PORT,
  ()=>console.log(
    `ZIQVONA V1 running on port ${PORT}`
  )
);
