import { useEffect, useState, type Dispatch, type SetStateAction } from "react";

export function useAutoDismissMessage(
  message: string | null,
  setMessage: Dispatch<SetStateAction<string | null>>,
  timeoutMs = 5000,
  fadeMs = 400
) {
  const [fading, setFading] = useState(false);

  useEffect(() => {
    if (!message) {
      setFading(false);
      return;
    }

    setFading(false);
    const fadeDelay = Math.max(0, timeoutMs - fadeMs);
    const fadeTimer = window.setTimeout(() => setFading(true), fadeDelay);
    const clearTimer = window.setTimeout(() => setMessage(null), timeoutMs);

    return () => {
      window.clearTimeout(fadeTimer);
      window.clearTimeout(clearTimer);
    };
  }, [message, setMessage, timeoutMs, fadeMs]);

  return fading;
}
