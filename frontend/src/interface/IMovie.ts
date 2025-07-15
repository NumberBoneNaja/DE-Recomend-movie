export interface Movie {
    id?: number;
    title?: string;
    overview?: string;
    release_date?: string; // ใช้ string ถ้าได้มาจาก JSON (เช่น "2022-06-20")
    original_language?: string;
    popularity?: number;
    vote_average?: number;
    vote_count?: number;
    adult?: boolean;
    poster_path?: string ;
    backdrop_path?: string ;
  }
  