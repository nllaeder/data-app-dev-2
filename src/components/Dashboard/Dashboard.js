
import React from 'react';

const Dashboard = () => {
  return (
    <div>
      <h2>Dashboard</h2>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '1rem' }}>
        {/* Dynamically add components here */}
      </div>
    </div>
  );
};

export default Dashboard;
