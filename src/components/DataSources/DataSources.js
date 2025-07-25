
import React from 'react';

const DataSources = () => {
  const services = [
    { name: 'Mailchimp', logo: 'mailchimp.png' },
    { name: 'Constant Contact', logo: 'constant-contact.png' },
  ];

  const handleConnect = (service) => {
    // Placeholder for OAuth flow
    console.log(`Connecting to ${service.name}`);
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
