import type { Movie } from "../../interface/IMovie";

interface recommendationsprops {
    recommendations: Movie[]
}
function ShowMovie({recommendations}: recommendationsprops) {
    return (
             <div>
             <div className=" p-4  w-full grid grid-cols-3 gap-10">
           {
            recommendations.map((movie: Movie) => (
                <div key={movie.id} className=" py-8 shadow-sm flex flex-col justify-center items-center rounded-3xl bg-white/10 backdrop-blur-sm">
                <div>
                    <img
                    src={`https://image.tmdb.org/t/p/w200${movie.poster_path}` }
                    className="w-[200px] h-auto rounded-2xl"
                    alt="Shoes" />
                </div>
                <div className="mt-4  flex flex-col justify-center items-center">
                    <p className="text-lg font-medium">{movie.title}</p>
                    <div className="mt-2 overflow-hidden whitespace-nowrap w-[200px] h-[48px]">
                        <p className="text-sm truncate">{movie.overview || "ไม่มีคำอธิบาย"}</p>
                        </div>
                    
                </div>
                   
               
                
                </div>
            ))
           }
            
        </div>
        </div>
       
    );
}
export default ShowMovie