import { use, useEffect, useState } from "react";
import Recommend from "../../component/recommend/recomend";
import type { Movie } from "../../interface/IMovie";
import {
  fetchAllMovies,
  fetchMovieById,
  fetchRecommendations,
} from "../../api";
import ShowMovie from "../../component/recommend/showMovie";
import Result from "../../component/recommend/result";


function Home() {
  const [allMovies, setAllMovies] = useState<Movie[]>([]);
  const [recommendations, setRecommendations] = useState<Movie[]>([]);
  const [movieDetails, setMovieDetails] = useState<Movie[]>([]);
  const [notFound, setNotFound] = useState(false);

  async function AllMovies() {
    const response = await fetchAllMovies();
    setAllMovies(response);
    console.log("allMovies:", allMovies);
  }

  async function fetchRecommends(title: string) {
    const res = await fetchRecommendations(title);
    if (res) {
      if (res.recommendations.length === 0) {
        setNotFound(true);
        setRecommendations([]);
        setMovieDetails([]);
        return;
      }

      setNotFound(false);
      setRecommendations(res.recommendations);
      console.log("recommendations is:", res.recommendations);
      fetchAllDetailsFromRecommendations(res.recommendations);
    } else {
      setNotFound(true);
      setRecommendations([]);
      setMovieDetails([]);
    }
  }
  async function fetchAllDetailsFromRecommendations(
    recommendations: { id: number }[]
  ) {
    try {
      const promises = recommendations.map(async (movie) => {
        try {
          console.log("movie_id :", movie.id);
          const detail = await fetchMovieById(movie.id);
          return detail;
        } catch (err) {
          console.error(" ดึงไม่ได้:", movie.id);
          return null;
        }
      });

      const results = await Promise.all(promises);
      const validResults = results.filter((r) => r !== null);
      setMovieDetails(validResults as Movie[]);
      console.log("movieDetails:", movieDetails);
    } catch (err) {
      console.error("ดึงไม่ได้:", err);
    }
  }

  useEffect(() => {
    AllMovies();
  }, []);

  useEffect(() => {
    
  }, [movieDetails, recommendations,allMovies]);

  return (
    <div className="flex flex-col justify-center items-center">
      <Recommend onSearch={fetchRecommends} />
      {notFound ? (
        <p className="text-red-500 text-xl mt-4">ไม่พบข้อมูล</p>
      ) : movieDetails.length > 0 ? (
        <Result recommendations={movieDetails} />
      ) : (
        <ShowMovie recommendations={allMovies} />
      )}
    </div>
  );
}

export default Home;
