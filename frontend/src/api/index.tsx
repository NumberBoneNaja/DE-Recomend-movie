export const fetchRecommendations = async (title: string) => {
    try {
      const response = await fetch(`http://localhost:8000/recommend?title=${encodeURIComponent(title)}`, {
        method: "GET",
        headers: {
          "Content-Type": "application/json"
        },
      });
  
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
  
      const result = await response.json();
      
      return result;
    } catch (error) {
      console.error("เกิดข้อผิดพลาด:", error);
      return null;
    }
  };

export const fetchAllMovies = async () => {
    try {
      const response = await fetch(`http://localhost:8000/movies`, {
        method: "GET",
        headers: {
          "Content-Type": "application/json"
        },
      });
  
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
  
      const result = await response.json();
      
      return result;
    } catch (error) {
      console.error("❌ เกิดข้อผิดพลาด:", error);
      return null;
    }
}

export const fetchMovieById = async ( id:number) => {
    try {
      const response = await fetch(`http://localhost:8000/movies/${id}`, {
        method: "GET",
        headers: {
          "Content-Type": "application/json"
        },
      });
  
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
  
      const result = await response.json();
      
      return result;
    } catch (error) {
      console.error("❌ เกิดข้อผิดพลาด:", error);
      return null;
    }
}
