import { useState } from "react";


function Recommend({onSearch}: any) {
    const [searchTerm, setSearchTerm] = useState("");

  const submit = () => {
    if (!searchTerm.trim()) return;
    onSearch(searchTerm);
  };
  const handleInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setSearchTerm(event.target.value);
  };
  
    return (
      <div className="w-full flex flex-col justify-center items-center gap-10 p-8">
        <div>
          <h1 className="text-2xl font-bold">Get Recommendations</h1>
        </div>
        <div className="w-full max-w-md flex gap-2">
          <label className="input flex items-center gap-2 border rounded-xl px-4 py-3 w-full shadow-md focus-within:ring-2 ring-blue-400">
            <svg
              className="h-5 w-5 opacity-50"
              xmlns="http://www.w3.org/2000/svg"
              viewBox="0 0 24 24"
            >
              <g
                strokeLinejoin="round"
                strokeLinecap="round"
                strokeWidth="2.5"
                fill="none"
                stroke="currentColor"
              >
                <circle cx="11" cy="11" r="8"></circle>
                <path d="m21 21-4.3-4.3"></path>
              </g>
            </svg>
            <input
              type="search"
              value={searchTerm}
              onChange={handleInputChange}
              required
              placeholder="Search"
              className="w-full bg-transparent outline-none text-lg h-auto"
            />
          </label>
          <button className="btn btn-soft" onClick={submit}>ค้นหา</button>
        </div>
      </div>
    );
  }
  
  export default Recommend;
  