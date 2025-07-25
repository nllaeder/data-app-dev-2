
import { initializeApp } from 'firebase/app';
import { getAuth, GoogleAuthProvider } from 'firebase/auth';

const firebaseConfig = {
  apiKey: "AIzaSyCvyncKr1xHcAI2_K4GRgPkcrBkDhJqmPc",
  authDomain: "sme-insights-hub-gjzjf.firebaseapp.com",
  projectId: "sme-insights-hub-gjzjf",
  storageBucket: "sme-insights-hub-gjzjf.firebasestorage.app",
  messagingSenderId: "673667740351",
  appId: "1:673667740351:web:038d842311775da4be8c0a"
};

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const googleProvider = new GoogleAuthProvider();

export { auth, googleProvider };
