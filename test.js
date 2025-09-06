require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
global.WebSocket = require('ws'); // add this line if Node < 18

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

(async () => {
  const channel = supabase.channel('test')
    .on('postgres_changes', { event: '*', schema: 'public' }, payload => {
      console.log('Change received!', payload);
    })
    .subscribe(status => console.log('Status:', status));
})();
