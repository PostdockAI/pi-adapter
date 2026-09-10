type PiContext = {
    cwd?: string;
    sessionManager?: {
        getSessionFile?: () => string | Promise<string>;
    };
    sendUserMessage: (content: string, options?: {
        deliverAs?: "steer" | "followUp";
    }) => Promise<unknown>;
    isStreaming?: boolean | (() => boolean);
    ui?: {
        notify?: (message: string, level?: string) => void;
    };
};
type PiExtensionApi = {
    registerCommand: (name: string, options: {
        description: string;
        handler: (args: string, ctx: PiContext) => Promise<void>;
    }) => void;
    on: (event: string, handler: (event: unknown, ctx: PiContext) => Promise<void> | void) => void;
};
export default function registerMyagent(pi: PiExtensionApi): void;
export {};
