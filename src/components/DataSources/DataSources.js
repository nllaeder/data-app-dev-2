
import React from 'react';

const DataSources = ({ user }) => {
  const services = [
    { name: 'Mailchimp', logo: 'mailchimp.png', source: 'mailchimp' },
    { name: 'Constant Contact', logo: 'constant-contact.png', source: 'constant-contact' },
  ];

  const handleConnect = (service) => {
    if (!user) {
      console.error("User not logged in.");
      return;
    }

    let authUrl;
    const state = user.uid;

    if (service.source === 'mailchimp') {
      // TODO: Replace with your actual Mailchimp Client ID
      const clientId = "YOUR_MAILCHIMP_CLIENT_ID";
      // TODO: Replace with the URL of your deployed OAuth callback function
      const redirectUri = "https://your-callback-url.cloudfunctions.net/mailchimp-oauth-callback";
      authUrl = `https://login.mailchimp.com/oauth2/authorize?client_id=${clientId}&redirect_uri=${redirectUri}&response_type=code&state=${state}`;
    } else if (service.source === 'constant-contact') {
      // TODO: Replace with your actual Constant Contact Client ID
      const clientId = "YOUR_CONSTANT_CONTACT_CLIENT_ID";
      // TODO: Replace with the URL of your deployed OAuth callback function
      const redirectUri = "https://your-callback-url.cloudfunctions.net/constant-contact-oauth-callback";
      authUrl = `https://id.constantcontact.com/as/authorization.oauth2?client_id=${clientId}&redirect_uri=${redirectUri}&response_type=code&scope=contact_data+campaign_data&state=${state}`;
    }

    if (authUrl) {
      window.location.href = authUrl;
    }
  };

  return (
    <div>
      <h2>Connect a New Data Source</h2>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: '1rem' }}>
        {services.map((service) => (
          <div key={service.name} style={{ border: '1px solid #ccc', padding: '1rem', textAlign: 'center' }}>
            <img src={service.logo} alt={`${service.name} logo`} style={{ maxWidth: '100px', maxHeight: '50px', marginBottom: '1rem' }} />
            <h3>{service.name}</h3>
            <button onClick={() => handleConnect(service)}>Connect</button>
          </div>
        ))}
      </div>
    </div>
  );
};

export default DataSources;

export default DataSources;
