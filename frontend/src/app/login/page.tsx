import { Suspense } from "react";
import { LoginPage } from "@/components/LoginPage";

export default function Login() {
  return (
    <Suspense fallback={null}>
      <LoginPage />
    </Suspense>
  );
}
